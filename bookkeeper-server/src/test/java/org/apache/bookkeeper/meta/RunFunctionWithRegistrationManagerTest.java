package org.apache.bookkeeper.meta;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
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
 * Method tested : runFunctionWithRegistrationManager
 * Martina Salvati
 */


@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(Parameterized.class)
@PrepareForTest({ZooKeeperClient.class})

public  class RunFunctionWithRegistrationManagerTest<T> {

    static MetadataClientDriver clientDriver = mock(MetadataClientDriver.class);
    static MetadataBookieDriver bookieDriver = mock(MetadataBookieDriver.class);
    static ClientConfiguration clientConf = mock(ClientConfiguration.class);
    static ServerConfiguration serverConf = mock(ServerConfiguration.class);
    static ScheduledExecutorService exec = mock(ScheduledExecutorService.class);
    static ZKMetadataClientDriver ZKcl = mock(ZKMetadataClientDriver.class);
    static ZooKeeperClient.Builder mockZkBuilder = mock(ZooKeeperClient.Builder.class);
    static ZooKeeperClient mockZkc = mock(ZooKeeperClient.class);
    private final String metadataBackendScheme;
    private final Function<RegistrationManager, T> function;
    private final Object res;
    private static Function<RegistrationManager, Void> myvalidfunction = RegistrationManager -> {
        return (Void) null;
    };

    public RunFunctionWithRegistrationManagerTest(String metadataBackendScheme, Function<RegistrationManager, T> function, Object res) {
        this.metadataBackendScheme = metadataBackendScheme;
        this.function = function;
        this.res = res;
    }

    @Before
    public void configure() throws MetadataException, ConfigurationException, InterruptedException, KeeperException, IOException, ExecutionException {

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
                {"zk+hierarchical://127.0.0.1/ledgers", null, ExecutionException.class},
                {"zk+hierarchical://127.0.0.1/ledgers", myvalidfunction, null},
                {"zk+hierarchical://127.0.0.1/ledgers89", myvalidfunction, MetadataException.class}, //adequacy
                {"zk+hierarchical://127.0.0.1/ledgers", null, ExecutionException.class}, //adequacy
        });
    }

    @Test
    public void testRunFunction() {
        try {
            MetadataDrivers.runFunctionWithRegistrationManager(serverConf, function);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertEquals(e.getClass(), res);
        }
    }
}