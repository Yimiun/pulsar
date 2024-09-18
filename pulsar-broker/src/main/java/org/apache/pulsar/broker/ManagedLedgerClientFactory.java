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
package org.apache.pulsar.broker;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import io.netty.channel.EventLoopGroup;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl.BookkeeperFactoryForCustomEnsemblePlacementPolicy;
import org.apache.bookkeeper.stats.NullStatsProvider;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.commons.configuration.Configuration;
import org.apache.pulsar.broker.stats.prometheus.metrics.PrometheusMetricsProvider;
import org.apache.pulsar.broker.storage.ManagedLedgerStorage;
import org.apache.pulsar.common.policies.data.EnsemblePlacementPolicyConfig;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 服务类，组合了ManagedLedgerFactory和BooKKeeper的交互逻辑，其中:
 * <li>本实现类是由配置文件决定，从接口中的静态方法中反射出来</li>
 * <li>本实现类提供一个默认BK客户端，使用默认配置</li>
 * <li>本实现类的ManagedLedgerFactory对象，实际上是依靠bookkeeperProvider创建的bk客户端，只是做了配置检测的代理过程，并且在这里加入了缓存</li>
 * 若不采用这种形式实现，可以让ManagedLedgerFactory的构造函数中，第二个bkFactory改成bookkeeperProvider，直接传给它，在里面做配置检测。
 * 相当于把缓存的逻辑挪到ManagedLedgerFactory的实现类中。
 * */
public class ManagedLedgerClientFactory implements ManagedLedgerStorage {

    private static final Logger log = LoggerFactory.getLogger(ManagedLedgerClientFactory.class);

    private ManagedLedgerFactory managedLedgerFactory;
    private BookKeeper defaultBkClient;
    private final AsyncCache<EnsemblePlacementPolicyConfig, BookKeeper>
            bkEnsemblePolicyToBkClientMap = Caffeine.newBuilder().buildAsync();
    private StatsProvider statsProvider = new NullStatsProvider();

    public void initialize(ServiceConfiguration conf, MetadataStoreExtended metadataStore,
                           BookKeeperClientFactory bookkeeperProvider,
                           EventLoopGroup eventLoopGroup) throws Exception {
        ManagedLedgerFactoryConfig managedLedgerFactoryConfig = new ManagedLedgerFactoryConfig();
        Configuration configuration = new ClientConfiguration();
        // 折叠代码为config转换逻辑，以及bk的普罗米修斯初始化逻辑
        {
            managedLedgerFactoryConfig.setMaxCacheSize(conf.getManagedLedgerCacheSizeMB() * 1024L * 1024L);
            managedLedgerFactoryConfig.setCacheEvictionWatermark(conf.getManagedLedgerCacheEvictionWatermark());
            managedLedgerFactoryConfig.setNumManagedLedgerSchedulerThreads(conf.getManagedLedgerNumSchedulerThreads());
            managedLedgerFactoryConfig.setCacheEvictionIntervalMs(conf.getManagedLedgerCacheEvictionIntervalMs());
            managedLedgerFactoryConfig.setCacheEvictionTimeThresholdMillis(
                    conf.getManagedLedgerCacheEvictionTimeThresholdMillis());
            managedLedgerFactoryConfig.setCopyEntriesInCache(conf.isManagedLedgerCacheCopyEntries());
            managedLedgerFactoryConfig.setManagedLedgerMaxReadsInFlightSize(
                    conf.getManagedLedgerMaxReadsInFlightSizeInMB() * 1024L * 1024L);
            managedLedgerFactoryConfig.setPrometheusStatsLatencyRolloverSeconds(
                    conf.getManagedLedgerPrometheusStatsLatencyRolloverSeconds());
            managedLedgerFactoryConfig.setTraceTaskExecution(conf.isManagedLedgerTraceTaskExecution());
            managedLedgerFactoryConfig.setCursorPositionFlushSeconds(conf.getManagedLedgerCursorPositionFlushSeconds());
            managedLedgerFactoryConfig.setManagedLedgerInfoCompressionType(conf.getManagedLedgerInfoCompressionType());
            managedLedgerFactoryConfig.setManagedLedgerInfoCompressionThresholdInBytes(
                    conf.getManagedLedgerInfoCompressionThresholdInBytes());
            managedLedgerFactoryConfig.setStatsPeriodSeconds(conf.getManagedLedgerStatsPeriodSeconds());
            managedLedgerFactoryConfig.setManagedCursorInfoCompressionType(conf.getManagedCursorInfoCompressionType());
            managedLedgerFactoryConfig.setManagedCursorInfoCompressionThresholdInBytes(
                    conf.getManagedCursorInfoCompressionThresholdInBytes());

            if (conf.isBookkeeperClientExposeStatsToPrometheus()) {
                configuration.addProperty(PrometheusMetricsProvider.PROMETHEUS_STATS_LATENCY_ROLLOVER_SECONDS,
                        conf.getManagedLedgerPrometheusStatsLatencyRolloverSeconds());
                configuration.addProperty(PrometheusMetricsProvider.CLUSTER_NAME, conf.getClusterName());
                statsProvider = new PrometheusMetricsProvider();
            }
            statsProvider.start(configuration);
        }
        StatsLogger statsLogger = statsProvider.getStatsLogger("pulsar_managedLedger_client");
        //根据配置创建了一个bookie客户端，作为默认的bookie客户端
        this.defaultBkClient =
                bookkeeperProvider.create(conf, metadataStore, eventLoopGroup, Optional.empty(), null, statsLogger)
                        .get();
        // 传入一个策略，作为配置策略的bookie客户端
        // bkFactory实际上就是接受一个创建策略，可以为空（默认）/自定义，缓存并返回创建的BookKeeper客户端
        // 其中，策略的hashcode是: 若Bookkeeper指定的EnsemblePlacementPolicy类名字相同，且属性一致，就返回相同客户端。
        // 这很好理解，就是要根据业务配置的不同属性（类似各种超时时间/副本数量/节点配置等），new出新的客户端，并且放在缓存中复用。
        BookkeeperFactoryForCustomEnsemblePlacementPolicy bkFactory = (
                EnsemblePlacementPolicyConfig ensemblePlacementPolicyConfig) -> {
            if (ensemblePlacementPolicyConfig == null || ensemblePlacementPolicyConfig.getPolicyClass() == null) {
                return CompletableFuture.completedFuture(defaultBkClient);
            }

            // find or create bk-client in cache for a specific ensemblePlacementPolicy
            // 一个咖啡因本地缓存，没有即创建，命中即返回。
            // 这个map里装的就是策略对应的Bookie客户端的异步缓存，即CompletableFuture<BookKeeper>
            return bkEnsemblePolicyToBkClientMap.get(ensemblePlacementPolicyConfig,
                    (key, executor) -> bookkeeperProvider.create(conf, metadataStore, eventLoopGroup,
                            Optional.ofNullable(ensemblePlacementPolicyConfig.getPolicyClass()),
                            ensemblePlacementPolicyConfig.getProperties(), statsLogger));
        };
        // 因此这个工厂逻辑如下：
        // 根据传入的初始化配置策略动态创建bookkeeper客户端
        // get方法返回默认的bookkeeper异步客户端，
        // get(ensemblePlacementPolicyConfig) 返回根据这个参数创建的bookkeeper异步客户端
        // 正常写应该是new 一个新的类，实现两种方法，把bkEnsemblePolicyToBkClientMap作为那个对象的成员变量，这里是图方便？
        // 其实唯一的区别就是bkEnsemblePolicyToBkClientMap这个缓存的持有者给了当前类，而不是BookkeeperFactoryForCustomEnsemblePlacementPolicy实现类
        // 哦，那有可能BookkeeperFactoryForCustomEnsemblePlacementPolicy的实现类很多，由于是接口，只能写static的cache对象保存实例，这样不是很好？

        try {
            this.managedLedgerFactory =
                    new ManagedLedgerFactoryImpl(metadataStore, bkFactory, managedLedgerFactoryConfig, statsLogger);
        } catch (Exception e) {
            statsProvider.stop();
            defaultBkClient.close();
            throw e;
        }
    }

    public ManagedLedgerFactory getManagedLedgerFactory() {
        return managedLedgerFactory;
    }

    public BookKeeper getBookKeeperClient() {
        return defaultBkClient;
    }

    public StatsProvider getStatsProvider() {
        return statsProvider;
    }

    @VisibleForTesting
    public Map<EnsemblePlacementPolicyConfig, BookKeeper> getBkEnsemblePolicyToBookKeeperMap() {
        return bkEnsemblePolicyToBkClientMap.synchronous().asMap();
    }

    @Override
    public void close() throws IOException {
        try {
            if (null != managedLedgerFactory) {
                managedLedgerFactory.shutdown();
                log.info("Closed managed ledger factory");
            }

            if (null != statsProvider) {
                statsProvider.stop();
            }
            try {
                if (null != defaultBkClient) {
                    defaultBkClient.close();
                }
            } catch (RejectedExecutionException ree) {
                // when closing bookkeeper client, it will error outs all pending metadata operations.
                // those callbacks of those operations will be triggered, and submitted to the scheduler
                // in managed ledger factory. but the managed ledger factory has been shutdown before,
                // so `RejectedExecutionException` will be thrown there. we can safely ignore this exception.
                //
                // an alternative solution is to close bookkeeper client before shutting down managed ledger
                // factory, however that might be introducing more unknowns.
                log.warn("Encountered exceptions on closing bookkeeper client", ree);
            }
            bkEnsemblePolicyToBkClientMap.synchronous().asMap().forEach((policy, bk) -> {
                try {
                    if (bk != null) {
                        bk.close();
                    }
                } catch (Exception e) {
                    log.warn("Failed to close bookkeeper-client for policy {}", policy, e);
                }
            });
            log.info("Closed BookKeeper client");
        } catch (Exception e) {
            log.warn(e.getMessage(), e);
            throw new IOException(e);
        }
    }
}
