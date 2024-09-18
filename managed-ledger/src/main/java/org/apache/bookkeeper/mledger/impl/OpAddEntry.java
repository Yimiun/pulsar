/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.mledger.impl;

import static com.google.common.base.Preconditions.checkArgument;
import io.netty.buffer.ByteBuf;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.netty.util.ReferenceCountUtil;

import java.lang.reflect.Field;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.AsyncCallback.CloseCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.ScanOutcome;
import org.apache.bookkeeper.mledger.intercept.ManagedLedgerInterceptor;


/**
 * Handles the life-cycle of an addEntry() operation.
 *
 */
@Slf4j
public class OpAddEntry implements AddCallback, CloseCallback, Runnable {
    protected ManagedLedgerImpl ml;
    LedgerHandle ledger;
    long entryId;
    private int numberOfMessages;

    @SuppressWarnings("unused")
    private static final AtomicReferenceFieldUpdater<OpAddEntry, AddEntryCallback> callbackUpdater =
            AtomicReferenceFieldUpdater.newUpdater(OpAddEntry.class, AddEntryCallback.class, "callback");
    protected volatile AddEntryCallback callback;
    Object ctx;
    volatile long addOpCount;
    private static final AtomicLongFieldUpdater<OpAddEntry> ADD_OP_COUNT_UPDATER = AtomicLongFieldUpdater
            .newUpdater(OpAddEntry.class, "addOpCount");
    private boolean closeWhenDone;
    private long startTime;
    volatile long lastInitTime;
    @SuppressWarnings("unused")
    ByteBuf data;
    private int dataLength;
    private ManagedLedgerInterceptor.PayloadProcessorHandle payloadProcessorHandle = null;

    private static final AtomicReferenceFieldUpdater<OpAddEntry, OpAddEntry.State> STATE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(OpAddEntry.class, OpAddEntry.State.class, "state");
    volatile State state;

    @Setter
    private AtomicBoolean timeoutTriggered;

    enum State {
        OPEN,
        INITIATED,
        COMPLETED,
        CLOSED
    }

    public static OpAddEntry createNoRetainBuffer(ManagedLedgerImpl ml, ByteBuf data, AddEntryCallback callback,
                                                  Object ctx, AtomicBoolean timeoutTriggered) {
        OpAddEntry op = createOpAddEntryNoRetainBuffer(ml, data, callback, ctx, timeoutTriggered);
        if (log.isDebugEnabled()) {
            log.debug("Created new OpAddEntry {}", op);
        }
        return op;
    }

    public static OpAddEntry createNoRetainBuffer(ManagedLedgerImpl ml, ByteBuf data, int numberOfMessages,
                                                  AddEntryCallback callback, Object ctx,
                                                  AtomicBoolean timeoutTriggered) {
        OpAddEntry op = createOpAddEntryNoRetainBuffer(ml, data, callback, ctx, timeoutTriggered);
        op.numberOfMessages = numberOfMessages;
        if (log.isDebugEnabled()) {
            log.debug("Created new OpAddEntry {}", op);
        }
        return op;
    }

    private static OpAddEntry createOpAddEntryNoRetainBuffer(ManagedLedgerImpl ml, ByteBuf data,
                                                             AddEntryCallback callback, Object ctx,
                                                             AtomicBoolean timeoutTriggered) {
        OpAddEntry op = RECYCLER.get();
        op.ml = ml;
        op.ledger = null;
        op.data = data;
        op.dataLength = data.readableBytes();
        op.callback = callback;
        op.ctx = ctx;
        op.addOpCount = ManagedLedgerImpl.ADD_OP_COUNT_UPDATER.incrementAndGet(ml);
        op.closeWhenDone = false;
        op.entryId = -1;
        op.startTime = System.nanoTime();
        op.state = State.OPEN;
        op.payloadProcessorHandle = null;
        op.timeoutTriggered = timeoutTriggered;
        ml.mbean.addAddEntrySample(op.dataLength);
        return op;
    }

    public void setLedger(LedgerHandle ledger) {
        this.ledger = ledger;
    }

    public void setCloseWhenDone(boolean closeWhenDone) {
        this.closeWhenDone = closeWhenDone;
    }

    public void initiate() {
        if (STATE_UPDATER.compareAndSet(OpAddEntry.this, State.OPEN, State.INITIATED)) {
            ByteBuf duplicateBuffer = data.retainedDuplicate();
            // retainDuplicate会复制一份bytebuf，但是底层的数组指向的还是老的那个,老的retain一次，+1，因此需要多释放一次。
            // 但是创建的新buffer的readerIndex和writerIndex是新的，不是老的，可以保证传入一个黑盒方法后，不会影响老buffer的可读性与可写性，
            // 不需要复杂的回调去重置index，因此这里采用复制的方式

            // 这次retain其实也是保证传递给下一个线程的方法前，要retain一次，那个方法负责释放。
            // netty中需要谁最后使用，谁就释放，每个黑盒方法不知道自己是不是最后使用这个buffer的，因此需要在最后释放。所以约定传入之前就retain一次。

            // internally asyncAddEntry() will take the ownership of the buffer and release it at the end
            // bookie客户端的recyclePendAddOpObject方法会最终release一次
            // pulsarDecoder的channelRead的finally块中会release一次
            // 自己用完了还得release一次，在run方法里
            addOpCount = ManagedLedgerImpl.ADD_OP_COUNT_UPDATER.incrementAndGet(ml);
            lastInitTime = System.nanoTime();
            if (ml.getManagedLedgerInterceptor() != null) {
                long originalDataLen = data.readableBytes();
                payloadProcessorHandle = ml.getManagedLedgerInterceptor().processPayloadBeforeLedgerWrite(this,
                        duplicateBuffer);
                if (payloadProcessorHandle != null) {
                    duplicateBuffer = payloadProcessorHandle.getProcessedPayload();
                    // If data len of entry changes, correct "dataLength" and "currentLedgerSize".
                    if (originalDataLen != duplicateBuffer.readableBytes()) {
                        this.dataLength = duplicateBuffer.readableBytes();
                        this.ml.currentLedgerSize += (dataLength - originalDataLen);
                    }
                }
            }
            // 自己作为回调传入方法
            ledger.asyncAddEntry(duplicateBuffer, this, addOpCount);
        } else {
            log.warn("[{}] initiate with unexpected state {}, expect OPEN state.", ml.getName(), state);
        }
    }

    public void initiateShadowWrite() {
        if (STATE_UPDATER.compareAndSet(OpAddEntry.this, State.OPEN, State.INITIATED)) {
            addOpCount = ManagedLedgerImpl.ADD_OP_COUNT_UPDATER.incrementAndGet(ml);
            lastInitTime = System.nanoTime();
            //Use entryId in PublishContext and call addComplete directly.
            this.addComplete(BKException.Code.OK, ledger, ((Position) ctx).getEntryId(), addOpCount);
        } else {
            log.warn("[{}] initiate with unexpected state {}, expect OPEN state.", ml.getName(), state);
        }
    }

    public void failed(ManagedLedgerException e) {
        AddEntryCallback cb = callbackUpdater.getAndSet(this, null);
        ml.afterFailedAddEntry(this.getNumberOfMessages());
        if (cb != null) {
            ReferenceCountUtil.release(data);
            cb.addFailed(e, ctx);
            ml.mbean.recordAddEntryError();
            if (payloadProcessorHandle != null) {
                payloadProcessorHandle.release();
            }
        }
    }

    @Override
    // rc r什么code,标识码
    // ledgerHandle
    public void addComplete(int rc, final LedgerHandle lh, long entryId, Object ctx) {
        if (!STATE_UPDATER.compareAndSet(OpAddEntry.this, State.INITIATED, State.COMPLETED)) {
            log.warn("[{}] The add op is terminal legacy callback for entry {}-{} adding.", ml.getName(), lh.getId(),
                    entryId);
            // Since there is a thread is coping this object, do not recycle this object to avoid other problems.
            // For example: we recycled this object, other thread get a null "opAddEntry.{variable_name}".
            // Recycling is not mandatory, JVM GC will collect it.
            return;
        }

        if (ledger != null && lh != null) {
            if (ledger.getId() != lh.getId()) {
                log.warn("[{}] ledgerId {} doesn't match with acked ledgerId {}", ml.getName(), ledger.getId(),
                        lh.getId());
            }
            checkArgument(ledger.getId() == lh.getId(), "ledgerId %s doesn't match with acked ledgerId %s",
                    ledger.getId(), lh.getId());
        }

        if (!checkAndCompleteOp(ctx)) {
            // means callback might have been completed by different thread (timeout task thread).. so do nothing
            return;
        }

        this.entryId = entryId;
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] write-complete: ledger-id={} entry-id={} size={} rc={}", this, ml.getName(),
                    lh == null ? -1 : lh.getId(), entryId, dataLength, rc);
        }

        if (rc != BKException.Code.OK || timeoutTriggered.get()) {
            // 若这是最后一条，处于切换状态，发送失败了，怎么处理？
            // 答：不用处理，若普通写入失败就直接关闭，触发重建重发逻辑，这个逻辑和最后一条的逻辑是一致的，都需要关闭当前写入的ledger，并且触发重建重发逻辑，
            // 因此 handleAddFailure里的ml.ledgerClosed(lh) 就可以认为是触发真正关闭，重发重建的唯一方法（况且名字就意味了一切2333）
            handleAddFailure(lh);
        } else {
            // Trigger addComplete callback in a thread hashed on the managed ledger name
            // 调用ml里的单线程执行run方法，相当于结束后的回调也全都用那个单线程执行，避免潜在问题。
            ml.getExecutor().execute(this);
        }
    }

    // Called in executor hashed on managed ledger name, once the add operation is complete
    // 单线程，一旦持久化写入到ledger后就执行
    @Override
    public void run() {
        if (payloadProcessorHandle != null) {
            payloadProcessorHandle.release();
        }
        // Remove this entry from the head of the pending queue
        OpAddEntry firstInQueue = ml.pendingAddEntries.poll();
        if (firstInQueue == null) {
            return;
        }
        if (this != firstInQueue) {
            // 由于添加到该队列的线程也是那个线程，因此消息会严格按顺序写入，每次从队列头部取出的一定是刚刚持久化的那一条，这种情况不是预期内的
            firstInQueue.failed(new ManagedLedgerException("Unexpected add entry op when complete the add entry op."));
            return;
        }

        ManagedLedgerImpl.NUMBER_OF_ENTRIES_UPDATER.incrementAndGet(ml);
        ManagedLedgerImpl.TOTAL_SIZE_UPDATER.addAndGet(ml, dataLength);

        long ledgerId = ledger != null ? ledger.getId() : ((Position) ctx).getLedgerId();
        // 应该是用于看有没有活跃消费者的，有就直接转发给消费者，省一次磁盘IO
        if (ml.hasActiveCursors()) {
            // Avoid caching entries if no cursor has been created
            EntryImpl entry = EntryImpl.create(ledgerId, entryId, data);
            // EntryCache.insert: duplicates entry by allocating new entry and data. so,
            // recycle entry after calling insert
            ml.entryCache.insert(entry);
            entry.release();
        }

        PositionImpl lastEntry = PositionImpl.get(ledgerId, entryId);
        ManagedLedgerImpl.ENTRIES_ADDED_COUNTER_UPDATER.incrementAndGet(ml);
        ml.lastConfirmedEntry = lastEntry;

        // 之前那个在是否切换ledger时set进去的属性，如果是true，意味着当前这个写完要切换ledger了
        if (closeWhenDone) {
            log.info("[{}] Closing ledger {} for being full", ml.getName(), ledgerId);
            // `data` will be released in `closeComplete`
            if (ledger != null) {
                ledger.asyncClose(this, ctx);
            }
        } else {
            updateLatency();
            AddEntryCallback cb = callbackUpdater.getAndSet(this, null);
            if (cb != null) {
                cb.addComplete(lastEntry, data.asReadOnly(), ctx);
                // 提醒资源准备好了
                ml.notifyCursors();
                ml.notifyWaitingEntryCallBacks();
                ReferenceCountUtil.release(data);
                // 执行pulsar包装的bk线程的回收 rnf再次-1（上一次是在bookie发送的客户端中，class类封装的发送方法会减一）
                this.recycle();
            } else {
                ReferenceCountUtil.release(data);
            }
        }
    }

    // closeLedger的回调函数，若当前消息是当前ledger的最后一条消息，会在消息成功写入的回调中调用ledger.asyncClose(this, ctx); 触发回调
    // 这个callback是由metadata-store线程池触发的，是zk的元数据线程
    @Override
    public void closeComplete(int rc, LedgerHandle lh, Object ctx) {
        checkArgument(ledger.getId() == lh.getId(), "ledgerId %s doesn't match with acked ledgerId %s", ledger.getId(),
                lh.getId());

        if (rc == BKException.Code.OK) {
            log.debug("Successfully closed ledger {}", lh.getId());
        } else {
            log.warn("Error when closing ledger {}. Status={}", lh.getId(), BKException.getMessage(rc));
        }

        ml.ledgerClosed(lh);
        updateLatency();

        // 这条消息是当前ledger的最后一条了，发送成功了，且状态是closewhendown（managed-ledger切换，处于closingLedger状态）才会调用到本方法
        // 因此这条消息的后续处理和正常的发送成功是一样的。
        AddEntryCallback cb = callbackUpdater.getAndSet(this, null);
        if (cb != null) {
            cb.addComplete(PositionImpl.get(lh.getId(), entryId), data.asReadOnly(), ctx);
            ml.notifyCursors();
            ml.notifyWaitingEntryCallBacks();
            ReferenceCountUtil.release(data);
            this.recycle();
        } else {
            ReferenceCountUtil.release(data);
        }
    }

    private void updateLatency() {
        ml.mbean.addAddEntryLatencySample(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
        ml.mbean.addLedgerAddEntryLatencySample(System.nanoTime() - lastInitTime, TimeUnit.NANOSECONDS);
    }

    /**
     * Checks if add-operation is completed.
     *
     * @return true if task is not already completed else returns false.
     */
    private boolean checkAndCompleteOp(Object ctx) {
        long addOpCount = (ctx instanceof Long) ? (long) ctx : -1;
        if (addOpCount != -1 && ADD_OP_COUNT_UPDATER.compareAndSet(this, addOpCount, -1)) {
            return true;
        }
        log.info("Add-entry already completed for {}-{}", ledger != null ? ledger.getId() : -1, entryId);
        return false;
    }

    /**
     * It handles add failure on the given ledger. it can be triggered when add-entry fails or times out.
     *
     * @param lh
     */
    void handleAddFailure(final LedgerHandle lh) {
        // If we get a write error, we will try to create a new ledger and re-submit the pending writes. If the
        // ledger creation fails (persistent bk failure, another instance owning the ML, ...), then the writes will
        // be marked as failed.
        ManagedLedgerImpl finalMl = this.ml;
        finalMl.mbean.recordAddEntryError();

        finalMl.getExecutor().execute(() -> {
            // Force the creation of a new ledger. Doing it in a background thread to avoid acquiring ML lock
            // from a BK callback.
            // 若在ml回调的线程里请求ml锁， 则有可能出现死锁（？），该回调线程负责处理该managed-ledger的回调数据，但是又在当前上下文请求锁
            // 保底也是一个性能损耗
            finalMl.ledgerClosed(lh);
        });
    }

    OpAddEntry duplicateAndClose(AtomicBoolean timeoutTriggered) {
        STATE_UPDATER.set(OpAddEntry.this, State.CLOSED);
        OpAddEntry duplicate =
                OpAddEntry.createNoRetainBuffer(ml, data, getNumberOfMessages(), callback, ctx, timeoutTriggered);
        return duplicate;
    }

    public State getState() {
        return state;
    }

    public ByteBuf getData() {
        return data;
    }

    public int getNumberOfMessages() {
        return numberOfMessages;
    }

    public Object getCtx() {
        return ctx;
    }

    public void setNumberOfMessages(int numberOfMessages) {
        this.numberOfMessages = numberOfMessages;
    }

    public void setData(ByteBuf data) {
        this.dataLength = data.readableBytes();
        this.data = data;
    }

    private final Handle<OpAddEntry> recyclerHandle;

    private OpAddEntry(Handle<OpAddEntry> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    private static final Recycler<OpAddEntry> RECYCLER = new Recycler<OpAddEntry>() {
        @Override
        protected OpAddEntry newObject(Recycler.Handle<OpAddEntry> recyclerHandle) {
            return new OpAddEntry(recyclerHandle);
        }
    };

    public void recycle() {
        ml = null;
        ledger = null;
        data = null;
        numberOfMessages = 0;
        dataLength = -1;
        callback = null;
        ctx = null;
        addOpCount = -1;
        closeWhenDone = false;
        entryId = -1;
        startTime = -1;
        lastInitTime = -1;
        payloadProcessorHandle = null;
        timeoutTriggered = null;
        recyclerHandle.recycle(this);
    }

    @Override
    public String toString() {
        ManagedLedgerImpl ml = this.ml;
        LedgerHandle ledger = this.ledger;
        return "OpAddEntry{"
                + "mlName=" + (ml != null ? ml.getName() : "null")
                + ", ledgerId=" + (ledger != null ? String.valueOf(ledger.getId()) : "null")
                + ", entryId=" + entryId
                + ", startTime=" + startTime
                + ", dataLength=" + dataLength
                + '}';
    }
}
