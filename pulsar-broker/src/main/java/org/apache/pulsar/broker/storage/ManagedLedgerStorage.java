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
package org.apache.pulsar.broker.storage;

import io.netty.channel.EventLoopGroup;
import java.io.IOException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.pulsar.broker.BookKeeperClientFactory;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.classification.InterfaceAudience.Private;
import org.apache.pulsar.common.classification.InterfaceStability.Unstable;
import org.apache.pulsar.common.util.Reflections;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

/**
 * Storage to access {@link org.apache.bookkeeper.mledger.ManagedLedger}s.
 */
@Private
@Unstable
public interface ManagedLedgerStorage extends AutoCloseable {

    /**
     * Initialize the managed ledger storage.
     *
     * @param conf service config
     * @param bookkeeperProvider bookkeeper provider
     * @throws Exception
     */
    void initialize(ServiceConfiguration conf,
                    MetadataStoreExtended metadataStore,
                    BookKeeperClientFactory bookkeeperProvider,
                    EventLoopGroup eventLoopGroup) throws Exception;

    /**
     * Return the factory to create {@link ManagedLedgerFactory}.
     *
     * @return the factory to create {@link ManagedLedgerFactory}.
     */
    ManagedLedgerFactory getManagedLedgerFactory();

    /**
     * Return the stats provider to expose the stats of the storage implementation.
     *
     * @return the stats provider.
     */
    StatsProvider getStatsProvider();

    /**
     * Return the default bookkeeper client.
     *
     * @return the default bookkeeper client.
     */
    BookKeeper getBookKeeperClient();

    /**
     * Close the storage.
     *
     * @throws IOException
     */
    void close() throws IOException;

    /**
     * Initialize the {@link ManagedLedgerStorage} from the provided resources.
     * <P>根据配置反射出一个Storage工厂实现类 即本接口实现类</P>
     * <P>为什么用这种形式? 因为可以在接口里强制你的实现类实现initial方法，并在这里调用，强制统一了实现类</P>
     * <P>如果采用子类自行实现构造函数式实现，还需要在这个方法里对构造函数参数校验才能反射，如果某个实现类未对齐参数，那就会出现异常</P>
     * <P>因此这种方式要优于构造函数反射</P>
     * @param conf service config
     * @param bkProvider bookkeeper client provider
     * @return the initialized managed ledger storage.
     */
    static ManagedLedgerStorage create(ServiceConfiguration conf,
                                       MetadataStoreExtended metadataStore,
                                       BookKeeperClientFactory bkProvider,
                                       EventLoopGroup eventLoopGroup) throws Exception {
        // 返回一个配置中的，ManagedLedgerStorage的实现类
        ManagedLedgerStorage storage =
                Reflections.createInstance(conf.getManagedLedgerStorageClassName(), ManagedLedgerStorage.class,
                        Thread.currentThread().getContextClassLoader());
        storage.initialize(conf, metadataStore, bkProvider, eventLoopGroup);
        return storage;
    }

}
