package org.apache.bookkeeper.meta;
import static org.apache.bookkeeper.meta.MetadataDrivers.BK_METADATA_BOOKIE_DRIVERS_PROPERTY;
import static org.apache.bookkeeper.meta.MetadataDrivers.BK_METADATA_CLIENT_DRIVERS_PROPERTY;
import static org.apache.bookkeeper.meta.MetadataDrivers.ZK_BOOKIE_DRIVER_CLASS;
import static org.apache.bookkeeper.meta.MetadataDrivers.ZK_CLIENT_DRIVER_CLASS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.mockito.ArgumentMatchers.eq;
import org.mockito.Spy;
import org.mockito.internal.matchers.Any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.OngoingStubbing;
import org.powermock.api.mockito.PowerMockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.mockito.ArgumentMatchers.eq;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.discover.RegistrationManager.RegistrationListener;
import org.apache.bookkeeper.meta.MetadataDriverTestConfiguration.BookieDriver1;
import org.apache.bookkeeper.meta.MetadataDriverTestConfiguration.BookieDriver2;
import org.apache.bookkeeper.meta.MetadataDriverTestConfiguration.ClientDriver1;
import org.apache.bookkeeper.meta.MetadataDriverTestConfiguration.ClientDriver2;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.meta.zk.ZKMetadataClientDriver;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.zookeeper.RetryPolicy;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.KeeperException;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;

/**
 * Unit test of {@link MetadataDrivers}.
 * Martina Salvati
 */

@RunWith(Enclosed.class)
public class MetadataDriversTest {
    static final Logger log = LoggerFactory.getLogger(MetadataDriversTest.class);

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
		      return java.util.Arrays.asList(new Object[][] {
		    	  {"zk",null},
		    	  {((String) null),NullPointerException.class},
		    	  {((URI) null),NullPointerException.class},
		    	  {"unknown",IllegalArgumentException.class},
		    	  {"Zk",null},
		    	  {"zK",null},
		    	  {"ZK",null}
		      }); 
	}
		  @Test
		  public void testGetClientDriverException() {
			  try {
				MetadataDrivers.getClientDriver(scheme);
		        assertEquals(ZK_CLIENT_DRIVER_CLASS, MetadataDrivers.getClientDriver(scheme).getClass().getName());

			} catch (Exception e) {
                Assert.assertEquals(e.getClass(), res);
			}
		  }
		  @Test
		  public void testGetBookieDriverException() {
			  try {
				  assertEquals(ZK_BOOKIE_DRIVER_CLASS,MetadataDrivers.getBookieDriver(scheme).getClass().getName());
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
		      return java.util.Arrays.asList(new Object[][] {
	        	  {"zk+hierarchical://127.0.0.1/ledgers",null},
	        	  {"hierarchical://127.0.0.1/ledgers",IllegalArgumentException.class},
		    	  {("unknown://"),IllegalArgumentException.class},
	        	  {("//127.0.0.1/ledgers"),NullPointerException.class},
	        	  {(String) null,NullPointerException.class},
	        	  {(URI) null, NullPointerException.class},
	        	  {"bk://localhost:2181:3181/path/to/namespace",IllegalArgumentException.class},
	        	  {"bk://localhost:-2181/path/to/namespace",IllegalArgumentException.class}
		      }); 
	}
		  @Test
		  public void testGetClientDriverException() {
			  try {
		        assertEquals(ZK_CLIENT_DRIVER_CLASS, MetadataDrivers.getClientDriver(URI.create(uri)).getClass().getName());

			} catch (Exception e) {
                Assert.assertEquals(e.getClass(), res);
			}
		  }
		  @Test
		  public void testGetBookieDriverException() {
			  try {
				  assertEquals(ZK_BOOKIE_DRIVER_CLASS,MetadataDrivers.getBookieDriver(URI.create(uri)).getClass().getName());
			} catch (Exception e) {
                Assert.assertEquals(e.getClass(), res);
			}
		  }

    }
    
   	public static class testRegisterClientAndBookieDriver {
    	
		static MetadataClientDriver clientDriver= mock(MetadataClientDriver.class);
		static MetadataBookieDriver bookieDriver= mock(MetadataBookieDriver.class);

   	    @BeforeClass
   		public static void settingMock() {
           when(clientDriver.getScheme()).thenReturn("testdriver");
           when(bookieDriver.getScheme()).thenReturn("testdriver");
   		}
   		
   		@Test
   		public void testRegisterClientDriver1() {
   		  try {
			MetadataDrivers.getClientDriver(clientDriver.getScheme());
            fail("Should fail to get client driver if it is not registered");
		} catch (Exception e) {
			
		}
   		  MetadataDrivers.registerClientDriver(clientDriver.getScheme(), clientDriver.getClass());
          MetadataClientDriver driver = MetadataDrivers.getClientDriver(clientDriver.getScheme());
          assertEquals(clientDriver.getClass(), driver.getClass());
   		}
   		
   		@Test
   		public void testRegisterBookieDriver1() {
   		 try {
             MetadataDrivers.getBookieDriver(bookieDriver.getScheme());
             fail("Should fail to get bookie driver if it is not registered");
         } catch (IllegalArgumentException iae) {
             // expected
         }
         MetadataDrivers.registerBookieDriver(bookieDriver.getScheme(), bookieDriver.getClass());
         MetadataBookieDriver driver = MetadataDrivers.getBookieDriver(bookieDriver.getScheme());
         assertEquals(bookieDriver.getClass(), driver.getClass());
    }
    }
	public static class testLoadFromSystemProperty {  
      

		    @Test
		    public void testLoadClientDriverFromSystemProperty() throws Exception {
		        String saveDriversStr = System.getProperty(BK_METADATA_CLIENT_DRIVERS_PROPERTY);
		        try {
		            System.setProperty(BK_METADATA_CLIENT_DRIVERS_PROPERTY, StringUtils
		                    .join(new String[] { ClientDriver1.class.getName(), ClientDriver2.class.getName() }, ':'));
		            MetadataDrivers.loadInitialDrivers();
		            MetadataClientDriver loadedDriver1 = MetadataDrivers.getClientDriver("driver1");
		            assertEquals(ClientDriver1.class, loadedDriver1.getClass());
		            MetadataClientDriver loadedDriver2 = MetadataDrivers.getClientDriver("driver2");
		            assertEquals(ClientDriver2.class, loadedDriver2.getClass());
		        } finally {
		            if (null != saveDriversStr) {
		                System.setProperty(BK_METADATA_CLIENT_DRIVERS_PROPERTY, saveDriversStr);
		            } else {
		                System.clearProperty(BK_METADATA_CLIENT_DRIVERS_PROPERTY);
		            }
		        }
		    }

		    @Test
		    public void testLoadBookieDriverFromSystemProperty() throws Exception {
		        String saveDriversStr = System.getProperty(BK_METADATA_BOOKIE_DRIVERS_PROPERTY);
		        try {
		            System.setProperty(BK_METADATA_BOOKIE_DRIVERS_PROPERTY, StringUtils.join(new String[] { BookieDriver1.class.getName(), BookieDriver2.class.getName() }, ':'));

		            MetadataDrivers.loadInitialDrivers();

		            MetadataBookieDriver loadedDriver1 = MetadataDrivers.getBookieDriver("driver1");
		            assertEquals(BookieDriver1.class, loadedDriver1.getClass());
		            MetadataBookieDriver loadedDriver2 = MetadataDrivers.getBookieDriver("driver2");
		            assertEquals(BookieDriver2.class, loadedDriver2.getClass());
		        } finally {
		            if (null != saveDriversStr) {
		                System.setProperty(BK_METADATA_BOOKIE_DRIVERS_PROPERTY, saveDriversStr);
		            } else {
		                System.clearProperty(BK_METADATA_BOOKIE_DRIVERS_PROPERTY);
		            }
		        }
		    }
	}
	public static class runFunctionWithMetadataClientDriverTest {
	   
		static MetadataClientDriver clientDriver= mock(MetadataClientDriver.class);
		static MetadataBookieDriver bookieDriver= mock(MetadataBookieDriver.class);
		static ClientConfiguration clientConf = mock(ClientConfiguration.class);
		static ServerConfiguration serverConf = mock(ServerConfiguration.class);
		static ZKMetadataDriverBase ZKdb = mock(ZKMetadataDriverBase.class);
		static RegistrationListener listener = mock(RegistrationListener.class);
		static ScheduledExecutorService exec = mock(ScheduledExecutorService.class);
		static ZKMetadataClientDriver ZKcl = mock(ZKMetadataClientDriver.class);
		static ZooKeeperClient.Builder mockZkBuilder = mock(ZooKeeperClient.Builder.class);
		static RetryPolicy rl = mock(RetryPolicy.class);
	    static ZooKeeperClient mockZkc = mock(ZooKeeperClient.class);

   	    @BeforeClass
   		public static void settingMock() throws ConfigurationException, MetadataException, KeeperException, InterruptedException, IOException {
          when(clientDriver.getScheme()).thenReturn("testdriver");
          when(bookieDriver.getScheme()).thenReturn("testdriver");
		  when(clientDriver.initialize(clientConf, exec, NullStatsLogger.INSTANCE, null)).thenReturn(clientDriver);
		  when(ZKcl.initialize(clientConf, exec, NullStatsLogger.INSTANCE, null)).thenReturn(ZKcl);
	      when(mockZkBuilder.connectString(eq("127.0.0.1"))).thenReturn(mockZkBuilder);
	      when(mockZkBuilder.sessionTimeoutMs(anyInt())).thenReturn(mockZkBuilder);
	      when(mockZkBuilder.requestRateLimit(anyDouble())).thenReturn(mockZkBuilder);
	      when(mockZkBuilder.statsLogger(any(StatsLogger.class))).thenReturn(mockZkBuilder);
	      when(mockZkBuilder.operationRetryPolicy(any(RetryPolicy.class))).thenReturn(mockZkBuilder);
	      when(mockZkc.exists(anyString(), eq(false))).thenReturn(null);
	      when(mockZkBuilder.build()).thenReturn(mockZkc);
            }
   	    
			/*
			 * @Test public void testrunFunctionWithMetadataClientDriver() throws Exception
			 * { when(clientConf.getMetadataServiceUri()).thenReturn("testdriver");
			 * when(MetadataDrivers.getBookieDriver("testdriver")).thenReturn(bookieDriver);
			 * MetadataDrivers.runFunctionWithMetadataClientDriver(clientConf, clientDriver
			 * -> { return 0; }, null); }
			 */
   	    
		@Test(expected = NullPointerException.class)
		public void testExceptionUriClient() throws MetadataException, ExecutionException, ConfigurationException {
			  when(clientConf.getMetadataServiceUri()).thenReturn("");
			  MetadataDrivers.runFunctionWithMetadataClientDriver(clientConf, clientDriver -> {
	                return 0;
			  }, null);
		}
		@Test(expected = NullPointerException.class)
		public void testExceptionUriBookie() throws MetadataException, ExecutionException, ConfigurationException {
			 when(serverConf.getMetadataServiceUri()).thenReturn("");
			  MetadataDrivers.runFunctionWithMetadataBookieDriver(serverConf, bookieDriver -> {
				  return 0;
			  });
		}
		@Test(expected = NullPointerException.class)
		public void testExceptionUriLedgerManagerFactory() throws MetadataException, ExecutionException, ConfigurationException {
			 when(serverConf.getMetadataServiceUri()).thenReturn("");
			  MetadataDrivers.runFunctionWithLedgerManagerFactory(serverConf, bookieDriver -> {
				  return 0;
			  });
		}
		@Test(expected = NullPointerException.class)
		public void testExceptionUrihRegistrationManager() throws MetadataException, ExecutionException, ConfigurationException {
			  when(serverConf.getMetadataServiceUri()).thenReturn("");
			  MetadataDrivers.runFunctionWithRegistrationManager(serverConf, bookieDriver -> {
				  return 0;
			  });
		}
		@Test(expected = NullPointerException.class)
		public void testExceptionUriLedgerManager() throws MetadataException, ExecutionException, ConfigurationException {
			  when(serverConf.getMetadataServiceUri()).thenReturn("");
			  MetadataDrivers.runFunctionWithLedgerManagerFactory(serverConf, bookieDriver -> {
				  return 0;
			  });
		}
	}
 

}
