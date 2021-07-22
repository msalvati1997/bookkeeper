package org.apache.bookkeeper.meta;

import org.apache.bookkeeper.meta.MetadataDriverTestConfiguration.BookieDriver1;
import org.apache.bookkeeper.meta.MetadataDriverTestConfiguration.BookieDriver2;
import org.apache.bookkeeper.meta.MetadataDriverTestConfiguration.ClientDriver1;
import org.apache.bookkeeper.meta.MetadataDriverTestConfiguration.ClientDriver2;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.net.URI;
import java.util.Collection;

import static org.apache.bookkeeper.meta.MetadataDrivers.*;
import static org.junit.Assert.assertEquals;

/**
 * Unit test of @link MetadataDrivers
 * Method tested : getClientDriver/getBookieDriver
 * Martina Salvati
 */

@RunWith(Enclosed.class)
public class getClientAndBookieDriverTest {


	@RunWith(Parameterized.class)
	public static class getClientAndBookieDriverFromSchemeTest {

		public getClientAndBookieDriverFromSchemeTest(String scheme, Object res) {
			super();
			this.scheme = scheme;
			this.res = res;
		}

		private final String scheme;
		private final Object res;

		@Parameterized.Parameters
		public static Collection data() {
			return java.util.Arrays.asList(new Object[][]{
					{"zk", null},
					{((String) null), NullPointerException.class},
					{((URI) null), NullPointerException.class},
					{"unknown", IllegalArgumentException.class},
					{"Zk", null},
					{"zK", null},
					{"ZK", null}
			});
		}

		@Test
		public void testGetClientDriver() {
			try {
				MetadataDrivers.getClientDriver(scheme);
				assertEquals(ZK_CLIENT_DRIVER_CLASS, MetadataDrivers.getClientDriver(scheme).getClass().getName());
			} catch (Exception e) {
				Assert.assertEquals(e.getClass(), res);
			}
		}
		@Test
		public void testGetBookieDriver() {
			try {
				assertEquals(ZK_BOOKIE_DRIVER_CLASS, MetadataDrivers.getBookieDriver(scheme).getClass().getName());
			} catch (Exception e) {
				Assert.assertEquals(e.getClass(), res);
			}
		}
	}

	@RunWith(Parameterized.class)
	public static class getClientAndBookieDriverFromURI {

		public getClientAndBookieDriverFromURI(String uri, Object res) {
			super();
			this.uri = uri;
			this.res = res;
		}

		private final String uri;
		private final Object res;

		@Parameterized.Parameters
		public static Collection data() {
			return java.util.Arrays.asList(new Object[][]{
					{"zk+hierarchical://127.0.0.1/ledgers", null},
					{"hierarchical://127.0.0.1/ledgers", IllegalArgumentException.class},
					{("unknown://"), IllegalArgumentException.class},
					{("//127.0.0.1/ledgers"), NullPointerException.class},
					{(String) null, NullPointerException.class},
					{(URI) null, NullPointerException.class},
					{"bk://localhost:2181:3181/path/to/namespace", IllegalArgumentException.class},
					{"bk://localhost:-2181/path/to/namespace", IllegalArgumentException.class},
					{"zk+", NullPointerException.class},
			});
		}

		@Test
		public void testGetClientDriver() {
			try {
				assertEquals(ZK_CLIENT_DRIVER_CLASS, MetadataDrivers.getClientDriver(URI.create(uri)).getClass().getName());
			} catch (Exception e) {
				Assert.assertEquals(e.getClass(), res);
			}
		}

		@Test
		public void testGetBookieDriver() {
			try {
				assertEquals(ZK_BOOKIE_DRIVER_CLASS, MetadataDrivers.getBookieDriver(URI.create(uri)).getClass().getName());
			} catch (Exception e) {
				Assert.assertEquals(e.getClass(), res);
			}
		}

	}

//	public static class testLoadFromSystemProperty {
//
//		@Test
//		public void testLoadClientDriverFromSystemProperty() {
//			String saveDriversStr = System.getProperty(BK_METADATA_CLIENT_DRIVERS_PROPERTY);
//			try {
//				System.setProperty(BK_METADATA_CLIENT_DRIVERS_PROPERTY, StringUtils.join(new String[]{ClientDriver1.class.getName(), ClientDriver2.class.getName()}, ':'));
//				MetadataDrivers.loadInitialDrivers();
//				MetadataClientDriver loadedDriver1 = MetadataDrivers.getClientDriver("driver1");
//				assertEquals(ClientDriver1.class, loadedDriver1.getClass());
//				MetadataClientDriver loadedDriver2 = MetadataDrivers.getClientDriver("driver2");
//				assertEquals(ClientDriver2.class, loadedDriver2.getClass());
//			} finally {
//				if (null != saveDriversStr) {
//					System.setProperty(BK_METADATA_CLIENT_DRIVERS_PROPERTY, saveDriversStr);
//				} else {
//					System.clearProperty(BK_METADATA_CLIENT_DRIVERS_PROPERTY);
//				}
//			}
//		}
//
//		@Test
//		public void testLoadBookieDriverFromSystemProperty() throws Exception {
//			String saveDriversStr = System.getProperty(BK_METADATA_BOOKIE_DRIVERS_PROPERTY);
//			try {
//				System.setProperty(BK_METADATA_BOOKIE_DRIVERS_PROPERTY, StringUtils.join(new String[]{BookieDriver1.class.getName(), BookieDriver2.class.getName()}, ':'));
//				MetadataDrivers.loadInitialDrivers();
//				MetadataBookieDriver loadedDriver1 = MetadataDrivers.getBookieDriver("driver1");
//				assertEquals(BookieDriver1.class, loadedDriver1.getClass());
//				MetadataBookieDriver loadedDriver2 = MetadataDrivers.getBookieDriver("driver2");
//				assertEquals(BookieDriver2.class, loadedDriver2.getClass());
//			} finally {
//				if (null != saveDriversStr) {
//					System.setProperty(BK_METADATA_BOOKIE_DRIVERS_PROPERTY, saveDriversStr);
//				} else {
//					System.clearProperty(BK_METADATA_BOOKIE_DRIVERS_PROPERTY);
//				}
//			}
//		}
//	}
//
//	@RunWith(PowerMockRunner.class)
//	@PowerMockRunnerDelegate(Parameterized.class)
//	@PrepareForTest({ZooKeeperClient.class})
//	public static class runFunctionWithMetadataClientDriverTest<T> {
//
//		static MetadataClientDriver clientDriver = mock(MetadataClientDriver.class);
//		static MetadataBookieDriver bookieDriver = mock(MetadataBookieDriver.class);
//		static ClientConfiguration clientConf = mock(ClientConfiguration.class);
//		static ServerConfiguration serverConf = mock(ServerConfiguration.class);
//		static ScheduledExecutorService exec = mock(ScheduledExecutorService.class);
//		static ZKMetadataClientDriver ZKcl = mock(ZKMetadataClientDriver.class);
//		static ZooKeeperClient.Builder mockZkBuilder = mock(ZooKeeperClient.Builder.class);
//		static ZooKeeperClient mockZkc = mock(ZooKeeperClient.class);
//		private final String metadataBackendScheme;
//		private final ScheduledExecutorService executor;
//		private final Function<MetadataClientDriver, T> function ;
//		private final Object res;
//		private static Function<MetadataClientDriver, Void> myvalidfunction = clientDriver -> { return (Void) null;
//		};
//
//		private static <T extends Throwable> void myFunc(Class<MetadataException> exceptionType) throws MetadataException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException, Throwable, T {
//			final String message = "mymetadata";
//			throw exceptionType.getConstructor(String.class).newInstance(message);
//		}
//
//		public runFunctionWithMetadataClientDriverTest(String metadataBackendScheme, ScheduledExecutorService executor, Function<MetadataClientDriver, T> function, Object res) {
//			this.metadataBackendScheme = metadataBackendScheme;
//			this.executor = executor;
//			this.function = function;
//			this.res = res;
//		}
//
//		@Before
//		public void configure() throws MetadataException, ConfigurationException, InterruptedException, KeeperException, IOException, ExecutionException {
//			when(clientDriver.initialize(clientConf, exec, NullStatsLogger.INSTANCE, null)).thenReturn(clientDriver);
//			when(ZKcl.initialize(clientConf, exec, NullStatsLogger.INSTANCE, null)).thenReturn(ZKcl);
//			when(bookieDriver.getScheme()).thenReturn(metadataBackendScheme);
//			when(mockZkBuilder.connectString(eq("127.0.0.1"))).thenReturn(mockZkBuilder);
//			when(mockZkBuilder.sessionTimeoutMs(anyInt())).thenReturn(mockZkBuilder);
//			when(mockZkBuilder.requestRateLimit(anyDouble())).thenReturn(mockZkBuilder);
//			when(mockZkBuilder.statsLogger(any(StatsLogger.class))).thenReturn(mockZkBuilder);
//			when(mockZkBuilder.operationRetryPolicy(any(RetryPolicy.class))).thenReturn(mockZkBuilder);
//			when(mockZkc.exists(anyString(), eq(false))).thenReturn(null);
//			when(mockZkBuilder.build()).thenReturn(mockZkc);
//			when(serverConf.getMetadataServiceUri()).thenReturn(metadataBackendScheme);
//			when(clientConf.getMetadataServiceUri()).thenReturn(metadataBackendScheme);
//			mockStatic(ZooKeeperClient.class);
//			try {
//				PowerMockito.when(ZooKeeperClient.class, "newBuilder").thenReturn(mockZkBuilder);
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//		}
//		@After
//		public void tearDown() throws Exception {
//			if(executor!=null) {
//				executor.shutdown();
//			}
//		}
//
//		@Parameterized.Parameters
//		public static Collection data() {
//			return java.util.Arrays.asList(new Object[][]{
//					{"zk+hierarchical://127.0.0.1/ledgers",Executors.newSingleThreadScheduledExecutor(),null,ExecutionException.class},
//					{"zk+hierarchical://127.0.0.1/ledgers",null,myvalidfunction,null},
//					{"zk+hierarchical://127.0.0.1/ledgers",Executors.newSingleThreadScheduledExecutor(),myvalidfunction,null},
//					{"zk+hierarchical://127.0.0.1/ledgers",null,null,ExecutionException.class}, //adequacy
//			});
//		}
//
//		@Test
//		public void testRunFunction() {
//			try {
//				MetadataDrivers.runFunctionWithMetadataClientDriver(clientConf, function, executor);
//			}catch(Exception e) {
//				e.printStackTrace();
//				Assert.assertEquals(e.getClass(), res);
//			}
//		}
//	}
//	@RunWith(PowerMockRunner.class)
//	@PowerMockRunnerDelegate(Parameterized.class)
//	@PrepareForTest({ZooKeeperClient.class})
//	public static class runFunctionWithMetadataBookieDriverTest<T> {
//
//		static MetadataClientDriver clientDriver = mock(MetadataClientDriver.class);
//		static MetadataBookieDriver bookieDriver = mock(MetadataBookieDriver.class);
//		static ClientConfiguration clientConf = mock(ClientConfiguration.class);
//		static ServerConfiguration serverConf = mock(ServerConfiguration.class);
//		static ScheduledExecutorService exec = mock(ScheduledExecutorService.class);
//		static ZKMetadataClientDriver ZKcl = mock(ZKMetadataClientDriver.class);
//		static ZooKeeperClient.Builder mockZkBuilder = mock(ZooKeeperClient.Builder.class);
//		static ZooKeeperClient mockZkc = mock(ZooKeeperClient.class);
//		private final String metadataBackendScheme;
//		private final Function<MetadataBookieDriver, T> function ;
//		private final Object res;
//		private static Function<MetadataBookieDriver, Void> myvalidfunction = bookieDriver -> {
//			return (Void) null;
//		};
//
//		public runFunctionWithMetadataBookieDriverTest(String metadataBackendScheme,Function<MetadataBookieDriver, T> function, Object res) {
//			this.metadataBackendScheme = metadataBackendScheme;
//			this.function = function;
//			this.res = res;
//		}
//
//		@Before
//		public void configure() throws MetadataException, ConfigurationException, InterruptedException, KeeperException, IOException {
//
//			when(clientDriver.initialize(clientConf, exec, NullStatsLogger.INSTANCE, null)).thenReturn(clientDriver);
//			when(ZKcl.initialize(clientConf, exec, NullStatsLogger.INSTANCE, null)).thenReturn(ZKcl);
//			when(bookieDriver.getScheme()).thenReturn(metadataBackendScheme);
//			when(mockZkBuilder.connectString(eq("127.0.0.1"))).thenReturn(mockZkBuilder);
//			when(mockZkBuilder.sessionTimeoutMs(anyInt())).thenReturn(mockZkBuilder);
//			when(mockZkBuilder.requestRateLimit(anyDouble())).thenReturn(mockZkBuilder);
//			when(mockZkBuilder.statsLogger(any(StatsLogger.class))).thenReturn(mockZkBuilder);
//			when(mockZkBuilder.operationRetryPolicy(any(RetryPolicy.class))).thenReturn(mockZkBuilder);
//			when(mockZkc.exists(anyString(), eq(false))).thenReturn(null);
//			when(mockZkBuilder.build()).thenReturn(mockZkc);
//			when(serverConf.getMetadataServiceUri()).thenReturn(metadataBackendScheme);
//			when(clientConf.getMetadataServiceUri()).thenReturn(metadataBackendScheme);
//			mockStatic(ZooKeeperClient.class);
//			try {
//				PowerMockito.when(ZooKeeperClient.class, "newBuilder").thenReturn(mockZkBuilder);
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//		}
//
//		@Parameterized.Parameters
//		public static Collection data() {
//			return java.util.Arrays.asList(new Object[][]{
//					{"zk+hierarchical://127.0.0.1/ledgers",myvalidfunction,null},
//					{"zk+hierarchical://127.0.0.1/ledgers89",myvalidfunction,MetadataException.class},//adequacy
//					{"zk+hierarchical://127.0.0.1/ledgers",null,ExecutionException.class},//adequacy
//			});
//		}
//
//		@Test
//		public void testRunFunction() {
//			try {
//				MetadataDrivers.runFunctionWithMetadataBookieDriver(serverConf, function);
//			}catch(Exception e) {
//				e.printStackTrace();
//				Assert.assertEquals(e.getClass(), res);
//			}
//		}
//	}
//
//	@RunWith(PowerMockRunner.class)
//	@PowerMockRunnerDelegate(Parameterized.class)
//	@PrepareForTest({ZooKeeperClient.class})
//	public static class runFunctionWithRegistrationManagerTest<T> {
//
//		static MetadataClientDriver clientDriver = mock(MetadataClientDriver.class);
//		static MetadataBookieDriver bookieDriver = mock(MetadataBookieDriver.class);
//		static ClientConfiguration clientConf = mock(ClientConfiguration.class);
//		static ServerConfiguration serverConf = mock(ServerConfiguration.class);
//		static ScheduledExecutorService exec = mock(ScheduledExecutorService.class);
//		static ZKMetadataClientDriver ZKcl = mock(ZKMetadataClientDriver.class);
//		static ZooKeeperClient.Builder mockZkBuilder = mock(ZooKeeperClient.Builder.class);
//		static ZooKeeperClient mockZkc = mock(ZooKeeperClient.class);
//		private final String metadataBackendScheme;
//		private final Function<RegistrationManager, T> function ;
//		private final Object res;
//		private static Function<RegistrationManager, Void> myvalidfunction = RegistrationManager -> { return (Void) null; };
//
//		public runFunctionWithRegistrationManagerTest(String metadataBackendScheme, Function<RegistrationManager, T> function, Object res) {
//			this.metadataBackendScheme = metadataBackendScheme;
//			this.function = function;
//			this.res = res;
//		}
//
//		@Before
//		public void configure() throws MetadataException, ConfigurationException, InterruptedException, KeeperException, IOException, ExecutionException {
//
//			when(clientDriver.initialize(clientConf, exec, NullStatsLogger.INSTANCE, null)).thenReturn(clientDriver);
//			when(ZKcl.initialize(clientConf, exec, NullStatsLogger.INSTANCE, null)).thenReturn(ZKcl);
//			when(bookieDriver.getScheme()).thenReturn(metadataBackendScheme);
//			when(mockZkBuilder.connectString(eq("127.0.0.1"))).thenReturn(mockZkBuilder);
//			when(mockZkBuilder.sessionTimeoutMs(anyInt())).thenReturn(mockZkBuilder);
//			when(mockZkBuilder.requestRateLimit(anyDouble())).thenReturn(mockZkBuilder);
//			when(mockZkBuilder.statsLogger(any(StatsLogger.class))).thenReturn(mockZkBuilder);
//			when(mockZkBuilder.operationRetryPolicy(any(RetryPolicy.class))).thenReturn(mockZkBuilder);
//			when(mockZkc.exists(anyString(), eq(false))).thenReturn(null);
//			when(mockZkBuilder.build()).thenReturn(mockZkc);
//			when(serverConf.getMetadataServiceUri()).thenReturn(metadataBackendScheme);
//			when(clientConf.getMetadataServiceUri()).thenReturn(metadataBackendScheme);
//			mockStatic(ZooKeeperClient.class);
//			try {
//				PowerMockito.when(ZooKeeperClient.class, "newBuilder").thenReturn(mockZkBuilder);
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//		}
//		@Parameterized.Parameters
//		public static Collection data() {
//			return java.util.Arrays.asList(new Object[][]{
//					{"zk+hierarchical://127.0.0.1/ledgers",null,ExecutionException.class},
//					{"zk+hierarchical://127.0.0.1/ledgers",myvalidfunction,null},
//					{"zk+hierarchical://127.0.0.1/ledgers89",myvalidfunction,MetadataException.class}, //adequacy
//					{"zk+hierarchical://127.0.0.1/ledgers",null,ExecutionException.class}, //adequacy
//			});
//		}
//		@Test
//		public void testRunFunction() {
//			try {
//				MetadataDrivers.runFunctionWithRegistrationManager(serverConf, function);
//			}catch(Exception e) {
//				e.printStackTrace();
//				Assert.assertEquals(e.getClass(), res);
//			}
//		}
//	}
//	@RunWith(PowerMockRunner.class)
//	@PowerMockRunnerDelegate(Parameterized.class)
//	@PrepareForTest({ZooKeeperClient.class})
//	public static class runFunctionWithLedgerManagerFactoryTest<T> {
//
//		static MetadataClientDriver clientDriver = mock(MetadataClientDriver.class);
//		static MetadataBookieDriver bookieDriver = mock(MetadataBookieDriver.class);
//		static ClientConfiguration clientConf = mock(ClientConfiguration.class);
//		static ServerConfiguration serverConf = mock(ServerConfiguration.class);
//		static ScheduledExecutorService exec = mock(ScheduledExecutorService.class);
//		static ZKMetadataClientDriver ZKcl = mock(ZKMetadataClientDriver.class);
//		static ZooKeeperClient.Builder mockZkBuilder = mock(ZooKeeperClient.Builder.class);
//		static ZooKeeperClient mockZkc = mock(ZooKeeperClient.class);
//		static LedgerManagerFactory mockLMF = mock(LedgerManagerFactory.class);
//		private final String metadataBackendScheme;
//		private final ScheduledExecutorService executor;
//		private final Function<LedgerManagerFactory, T> function ;
//		private final Object res;
//		private static Function<MetadataBookieDriver, Void> myvalidfunction = driver -> {
//			return (Void)null;
//		};
//
//		public runFunctionWithLedgerManagerFactoryTest(String metadataBackendScheme, ScheduledExecutorService executor, Function<LedgerManagerFactory, T> function, Object res) {
//			this.metadataBackendScheme = metadataBackendScheme;
//			this.executor = executor;
//			this.function = function;
//			this.res = res;
//		}
//
//		@Before
//		public void configure() throws MetadataException, ConfigurationException, InterruptedException, KeeperException, IOException {
//
//			when(clientDriver.initialize(clientConf, exec, NullStatsLogger.INSTANCE, null)).thenReturn(clientDriver);
//			when(ZKcl.initialize(clientConf, exec, NullStatsLogger.INSTANCE, null)).thenReturn(ZKcl);
//			when(bookieDriver.getScheme()).thenReturn(metadataBackendScheme);
//			when(mockZkBuilder.connectString(eq("127.0.0.1"))).thenReturn(mockZkBuilder);
//			when(mockZkBuilder.sessionTimeoutMs(anyInt())).thenReturn(mockZkBuilder);
//			when(mockZkBuilder.requestRateLimit(anyDouble())).thenReturn(mockZkBuilder);
//			when(mockZkBuilder.statsLogger(any(StatsLogger.class))).thenReturn(mockZkBuilder);
//			when(mockZkBuilder.operationRetryPolicy(any(RetryPolicy.class))).thenReturn(mockZkBuilder);
//			when(mockZkc.exists(anyString(), eq(false))).thenReturn(null);
//			when(mockZkBuilder.build()).thenReturn(mockZkc);
//			when(serverConf.getMetadataServiceUri()).thenReturn(metadataBackendScheme);
//			when(clientConf.getMetadataServiceUri()).thenReturn(metadataBackendScheme);
//			when(bookieDriver.getLedgerManagerFactory()).thenReturn(mockLMF);
//			mockStatic(ZooKeeperClient.class);
//			try {
//				PowerMockito.when(ZooKeeperClient.class, "newBuilder").thenReturn(mockZkBuilder);
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//		}
//
//		@After
//		public void tearDown() throws Exception {
//			if(executor!=null) {
//				executor.shutdown();
//			}
//		}
//
//		@Parameterized.Parameters
//		public static Collection data() {
//			return java.util.Arrays.asList(new Object[][]{
//					{"zk+hierarchical://127.0.0.1/ledgers",Executors.newSingleThreadScheduledExecutor(),null,ExecutionException.class},
//				//	{"zk+hierarchical://127.0.0.1/ledgers",null,myvalidfunction,null},
//				//	{"zk+hierarchical://127.0.0.1/ledgers89",null,myvalidfunction,MetadataException.class}, //adequacy
//					{"zk+hierarchical://127.0.0.1/ledgers",null,null,ExecutionException.class} //adequacy
//			});
//		}
//
//		@Test
//		public void testRunFunctionClient() {
//			try {
//				MetadataDrivers.runFunctionWithLedgerManagerFactory(serverConf, function);
//			}catch(Exception e) {
//				e.printStackTrace();
//				Assert.assertEquals(e.getClass(), res);
//			}
//		}
//	}
//
//	@RunWith(PowerMockRunner.class)
//	@PrepareForTest({ZooKeeperClient.class})
//	public static class testConfigurationException<T> {
//
//		static MetadataClientDriver clientDriver = mock(MetadataClientDriver.class);
//		static MetadataBookieDriver bookieDriver = mock(MetadataBookieDriver.class);
//		static ClientConfiguration clientConf = mock(ClientConfiguration.class);
//		static ServerConfiguration serverConf = mock(ServerConfiguration.class);
//		static ScheduledExecutorService exec = mock(ScheduledExecutorService.class);
//		static ZKMetadataClientDriver ZKcl = mock(ZKMetadataClientDriver.class);
//		static ZooKeeperClient.Builder mockZkBuilder = mock(ZooKeeperClient.Builder.class);
//		static ZooKeeperClient mockZkc = mock(ZooKeeperClient.class);
//		static LedgerManagerFactory mockFactory = mock(LedgerManagerFactory.class, CALLS_REAL_METHODS);
//		private static Function<MetadataBookieDriver, Void> myvalidfunction1 = driver -> { return (Void)null; };
//		private static Function<MetadataClientDriver, Void> myvalidfunction2 = driver -> { return (Void)null; };
//		private static Function<LedgerManagerFactory, Void> myvalidfunction3 = driver -> { return (Void)null; };
//
//		@Before
//		public void configureWithException() throws MetadataException, InterruptedException, KeeperException, ConfigurationException, IOException {
//			when(serverConf.getMetadataServiceUri()).thenReturn("zk+null://127.0.0.1/path/to/ledgers");
//			when(clientConf.getMetadataServiceUri()).thenReturn("zk+null://127.0.0.1/path/to/ledgers");
//			when(clientDriver.initialize(clientConf, exec, NullStatsLogger.INSTANCE, null)).thenReturn(clientDriver);
//			when(ZKcl.initialize(clientConf, exec, NullStatsLogger.INSTANCE, null)).thenReturn(ZKcl);
//			when(clientConf.getMetadataServiceUri()).thenThrow(ConfigurationException.class);
//			when(serverConf.getMetadataServiceUri()).thenThrow(ConfigurationException.class);
//			when(bookieDriver.getScheme()).thenReturn("zk+null://127.0.0.1/path/to/ledgers");
//			when(mockZkBuilder.connectString(eq("127.0.0.1"))).thenReturn(mockZkBuilder);
//			when(mockZkBuilder.sessionTimeoutMs(anyInt())).thenReturn(mockZkBuilder);
//			when(mockZkBuilder.requestRateLimit(anyDouble())).thenReturn(mockZkBuilder);
//			when(mockZkBuilder.statsLogger(any(StatsLogger.class))).thenReturn(mockZkBuilder);
//			when(mockZkBuilder.operationRetryPolicy(any(RetryPolicy.class))).thenReturn(mockZkBuilder);
//			when(mockZkc.exists(anyString(), eq(false))).thenReturn(null);
//			when(mockZkBuilder.build()).thenReturn(mockZkc);
//			serverConf.setLedgerManagerFactoryClass(mockFactory.getClass());
//			clientConf.setLedgerManagerFactoryClass(mockFactory.getClass());
//			mockStatic(ZooKeeperClient.class);
//			try {
//				PowerMockito.when(ZooKeeperClient.class, "newBuilder").thenReturn(mockZkBuilder);
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//		}
//
//		@Test(expected = MetadataException.class)
//		public void testConfigurationExceptionClient() throws ExecutionException, MetadataException {
//			MetadataDrivers.runFunctionWithMetadataClientDriver(clientConf, myvalidfunction2,null);
//		}
//		@Test(expected = MetadataException.class)
//		public void testConfigurationExceptionBookie() throws ExecutionException, MetadataException {
//			MetadataDrivers.runFunctionWithMetadataBookieDriver(serverConf, myvalidfunction1);
//		}
//	}
}