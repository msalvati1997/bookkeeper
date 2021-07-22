package org.apache.bookkeeper.meta;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.meta.zk.ZKMetadataClientDriver;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.zookeeper.RetryPolicy;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.zookeeper.KeeperException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

/**
 * Unit test of @link MetadataDrivers
 * Method tested : runFunctionWithMetadataBookieDriver
 * Martina Salvati
 */


@RunWith(Enclosed.class)
public  class RunFunctionWithMetadataBookieDriverTest<T> {

    @RunWith(PowerMockRunner.class)
    @PowerMockRunnerDelegate(Parameterized.class)
    @PrepareForTest({ZooKeeperClient.class})
    public static class RunFunctionWithMetadataBookieDriverTestBase<T> {

        static MetadataClientDriver clientDriver = mock(MetadataClientDriver.class);
        static MetadataBookieDriver bookieDriver = mock(MetadataBookieDriver.class);
        static ClientConfiguration clientConf = mock(ClientConfiguration.class);
        static ServerConfiguration serverConf = mock(ServerConfiguration.class);
        static ScheduledExecutorService exec = mock(ScheduledExecutorService.class);
        static ZKMetadataClientDriver ZKcl = mock(ZKMetadataClientDriver.class);
        static ZooKeeperClient.Builder mockZkBuilder = mock(ZooKeeperClient.Builder.class);
        static ZooKeeperClient mockZkc = mock(ZooKeeperClient.class);
        private final String metadataBackendScheme;
        private final Function<MetadataBookieDriver, T> function;
        private final Object res;
        private static Function<MetadataBookieDriver, Void> myvalidfunction = bookieDriver -> {
            return (Void) null;
        };

        public RunFunctionWithMetadataBookieDriverTestBase(String metadataBackendScheme, Function<MetadataBookieDriver, T> function, Object res) {
            this.metadataBackendScheme = metadataBackendScheme;
            this.function = function;
            this.res = res;
        }

        @Before
        public void configure() throws MetadataException, ConfigurationException, InterruptedException, KeeperException, IOException {

            when(clientDriver.initialize(clientConf, exec, NullStatsLogger.INSTANCE, null)).thenReturn(clientDriver);
            when(ZKcl.initialize(clientConf, exec, NullStatsLogger.INSTANCE, null)).thenReturn(ZKcl);
            when(bookieDriver.getScheme()).thenReturn(metadataBackendScheme);
            when(mockZkBuilder.connectString(eq("127.0.0.1"))).thenReturn(mockZkBuilder);
            when(mockZkBuilder.sessionTimeoutMs(anyInt())).thenReturn(mockZkBuilder);
            when(mockZkBuilder.requestRateLimit(anyDouble())).thenReturn(mockZkBuilder);
            when(mockZkBuilder.statsLogger(any(StatsLogger.class))).thenReturn(mockZkBuilder);
            when(mockZkBuilder.operationRetryPolicy(any(RetryPolicy.class))).thenReturn(mockZkBuilder);
            when(mockZkc.exists(anyString(), eq(false))).thenReturn(null);
            when(mockZkBuilder.build()).thenReturn(mockZkc);
            when(serverConf.getMetadataServiceUri()).thenReturn(metadataBackendScheme);
            when(clientConf.getMetadataServiceUri()).thenReturn(metadataBackendScheme);
            mockStatic(ZooKeeperClient.class);
            try {
                PowerMockito.when(ZooKeeperClient.class, "newBuilder").thenReturn(mockZkBuilder);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Parameterized.Parameters
        public static Collection data() {
            return java.util.Arrays.asList(new Object[][]{
                    {"zk+hierarchical://127.0.0.1/ledgers", myvalidfunction, null},
                    {"zk+hierarchical://127.0.0.1/ledgers89", myvalidfunction, MetadataException.class},//adequacy
                    {"zk+hierarchical://127.0.0.1/ledgers", null, ExecutionException.class},//adequacy
            });
        }

        @Test
        public void testRunFunction() throws ConfigurationException {
            when(serverConf.getMetadataServiceUri()).thenReturn(metadataBackendScheme);
            try {
                MetadataDrivers.runFunctionWithMetadataBookieDriver(serverConf, function);
            } catch (Exception e) {
                e.printStackTrace();
                Assert.assertEquals(e.getClass(), res);
            }
        }
    }

    @RunWith(PowerMockRunner.class)
    @PrepareForTest({ZooKeeperClient.class})
    public static class RunFunctionConfigurationExceptionTestBookie<T> {

        static MetadataClientDriver clientDriver = mock(MetadataClientDriver.class);
        static MetadataBookieDriver bookieDriver = mock(MetadataBookieDriver.class);
        static ClientConfiguration clientConf = mock(ClientConfiguration.class);
        static ServerConfiguration serverConf = mock(ServerConfiguration.class);
        static ScheduledExecutorService exec = mock(ScheduledExecutorService.class);
        static ZKMetadataClientDriver ZKcl = mock(ZKMetadataClientDriver.class);
        static ZooKeeperClient.Builder mockZkBuilder = mock(ZooKeeperClient.Builder.class);
        static ZooKeeperClient mockZkc = mock(ZooKeeperClient.class);
        static LedgerManagerFactory mockFactory = mock(LedgerManagerFactory.class, CALLS_REAL_METHODS);
        private static Function<MetadataBookieDriver, Void> myvalidfunction1 = driver -> {
            return (Void) null;
        };


        @Before
        public void configureWithException() throws MetadataException, InterruptedException, KeeperException, ConfigurationException, IOException {
            when(serverConf.getMetadataServiceUri()).thenReturn("zk+null://127.0.0.1/path/to/ledgers");
            when(clientConf.getMetadataServiceUri()).thenReturn("zk+null://127.0.0.1/path/to/ledgers");
            when(clientDriver.initialize(clientConf, exec, NullStatsLogger.INSTANCE, null)).thenReturn(clientDriver);
            when(ZKcl.initialize(clientConf, exec, NullStatsLogger.INSTANCE, null)).thenReturn(ZKcl);
            when(clientConf.getMetadataServiceUri()).thenThrow(ConfigurationException.class);
            when(serverConf.getMetadataServiceUri()).thenThrow(ConfigurationException.class);
            when(bookieDriver.getScheme()).thenReturn("zk+null://127.0.0.1/path/to/ledgers");
            when(mockZkBuilder.connectString(eq("127.0.0.1"))).thenReturn(mockZkBuilder);
            when(mockZkBuilder.sessionTimeoutMs(anyInt())).thenReturn(mockZkBuilder);
            when(mockZkBuilder.requestRateLimit(anyDouble())).thenReturn(mockZkBuilder);
            when(mockZkBuilder.statsLogger(any(StatsLogger.class))).thenReturn(mockZkBuilder);
            when(mockZkBuilder.operationRetryPolicy(any(RetryPolicy.class))).thenReturn(mockZkBuilder);
            when(mockZkc.exists(anyString(), eq(false))).thenReturn(null);
            when(mockZkBuilder.build()).thenReturn(mockZkc);
            serverConf.setLedgerManagerFactoryClass(mockFactory.getClass());
            clientConf.setLedgerManagerFactoryClass(mockFactory.getClass());
            mockStatic(ZooKeeperClient.class);
            try {
                PowerMockito.when(ZooKeeperClient.class, "newBuilder").thenReturn(mockZkBuilder);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Test(expected = MetadataException.class)
        public void testConfigurationExceptionBookie() throws ExecutionException, MetadataException {
            MetadataDrivers.runFunctionWithMetadataBookieDriver(serverConf, myvalidfunction1);
        }
    }
}
