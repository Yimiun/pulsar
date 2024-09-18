package org.apache.pulsar.broker;

import io.netty.channel.EventLoopGroup;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.commons.configuration.Configuration;
import org.apache.distributedlog.BookKeeperClient;
import org.apache.pulsar.broker.stats.prometheus.metrics.PrometheusMetricsProvider;
import org.apache.pulsar.broker.storage.ManagedLedgerStorage;
import org.apache.pulsar.common.policies.data.EnsemblePlacementPolicyConfig;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class MappedManagedLedgerClientFactory implements ManagedLedgerStorage {

    private CompletableFuture<BookKeeper> defaultBookKeeper;

    private StatsProvider statsProvider;

    private final Map<EnsemblePlacementPolicyConfig, CompletableFuture<BookKeeper>> cache = new ConcurrentHashMap<>();

    private ManagedLedgerFactory managedLedgerFactory;

    @Override
    public void initialize(ServiceConfiguration conf, MetadataStoreExtended metadataStore,
                           BookKeeperClientFactory bookkeeperProvider, EventLoopGroup eventLoopGroup) throws Exception {
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
        this.defaultBookKeeper = bookkeeperProvider.create(conf, metadataStore, eventLoopGroup, Optional.empty(),
                null, statsLogger);

        managedLedgerFactory = new ManagedLedgerFactoryImpl(metadataStore, ensemblePlacementPolicyMetadata -> {
            if (ensemblePlacementPolicyMetadata == null) {
                return defaultBookKeeper;
            }
            return cache.computeIfAbsent(ensemblePlacementPolicyMetadata, k -> bookkeeperProvider.create(conf, metadataStore, eventLoopGroup,
                    Optional.of(ensemblePlacementPolicyMetadata.getPolicyClass()), ensemblePlacementPolicyMetadata.getProperties(), statsLogger));
        }, managedLedgerFactoryConfig, statsLogger);
    }

    @Override
    public ManagedLedgerFactory getManagedLedgerFactory() {
        return managedLedgerFactory;
    }

    @Override
    public StatsProvider getStatsProvider() {
        return statsProvider;
    }

    @Override
    public BookKeeper getBookKeeperClient() {
        return defaultBookKeeper.join();
    }

    @Override
    public void close() throws IOException {

    }
}
