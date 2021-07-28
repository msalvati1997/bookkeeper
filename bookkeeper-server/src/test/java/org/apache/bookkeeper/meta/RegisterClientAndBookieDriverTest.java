package org.apache.bookkeeper.meta;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.util.Collection;
import static org.apache.bookkeeper.meta.MetadataDrivers.getBookieDrivers;
import static org.apache.bookkeeper.meta.MetadataDrivers.getClientDrivers;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit test of @link MetadataDrivers
 * Method tested : registerClientDriver and registerBookieDriver
 * Martina Salvati
 */
@RunWith(Parameterized.class)

public class RegisterClientAndBookieDriverTest {

    static MetadataClientDriver clientDriver = mock(MetadataClientDriver.class);
    static MetadataBookieDriver bookieDriver = mock(MetadataBookieDriver.class);
    private final String metadataBackendScheme;
    private final Object res;
    private final boolean allowOverride;

    public RegisterClientAndBookieDriverTest(String metadataBackendScheme, Object res, boolean allowOverride) {
        this.metadataBackendScheme = metadataBackendScheme;
        this.res = res;
        this.allowOverride = allowOverride;
    }
    @Before
    public void configureMock() {
        when(clientDriver.getScheme()).thenReturn(metadataBackendScheme);
        when(bookieDriver.getScheme()).thenReturn(metadataBackendScheme);
    }
    @Parameterized.Parameters
    public static Collection data() {
        return java.util.Arrays.asList(new Object[][]{
                {"zk",null,false},
                //adequacy
                {"zk1",IllegalArgumentException.class,false},
                {"zk1",null,false},
                {"ZK1", IllegalArgumentException.class,false},
                {"zk+hierarchical://127.0.0.1/ledgers",IllegalArgumentException.class,false},
                {"zk",null,true},
                {"ZK1", IllegalArgumentException.class,true},
                {"zk+hierarchical://127.0.0.1/ledgers",IllegalArgumentException.class,true},
                {"zk1",IllegalArgumentException.class,true},
                {"zk+hierarchical://127.0.0.1/ledgers",null,false},
        });
    }
    @Test
    public void testRegisterClientDriver() {
        boolean registered=true;
        try {
            MetadataDrivers.getClientDriver(clientDriver.getScheme());
        } catch (Exception e) {
            Assert.assertEquals(e.getClass(), res);
            registered=false;
        }
        if(!allowOverride) {
            MetadataDrivers.registerClientDriver(clientDriver.getScheme(), clientDriver.getClass());
            MetadataClientDriver driver = MetadataDrivers.getClientDriver(clientDriver.getScheme());
            if (!registered) {
                assertEquals(clientDriver.getClass(), driver.getClass());
            }
            assertTrue(!getClientDrivers().isEmpty());
            MetadataDrivers.registerClientDriver(clientDriver.getScheme(), clientDriver.getClass(),false);
            if (!registered) {
                assertEquals(clientDriver.getClass(), driver.getClass());
            }
        }
        else {
            MetadataDrivers.registerClientDriver(clientDriver.getScheme(), clientDriver.getClass(),true);
            MetadataClientDriver driver = MetadataDrivers.getClientDriver(clientDriver.getScheme());
            if (!registered) {
                assertEquals(clientDriver.getClass(), driver.getClass());
            }
        }
        assertTrue(!getClientDrivers().isEmpty());
        }
    @Test
    public void testRegisterBookieDriver() {
        boolean registered=true;
        try {
            MetadataDrivers.getBookieDriver(bookieDriver.getScheme());
        } catch (Exception e) {
            Assert.assertEquals(e.getClass(), res);
            registered=false;
        }
        if(!allowOverride) {
            MetadataDrivers.registerBookieDriver(bookieDriver.getScheme(), bookieDriver.getClass());
            MetadataBookieDriver driver = MetadataDrivers.getBookieDriver(bookieDriver.getScheme());
            if (!registered) {
                assertEquals(bookieDriver.getClass(), driver.getClass());
            }
            assertTrue(!getBookieDrivers().isEmpty());
            MetadataDrivers.registerBookieDriver(bookieDriver.getScheme(), bookieDriver.getClass(),false);
            if (!registered) {
                assertEquals(bookieDriver.getClass(), driver.getClass());
            }
        }
        else {
            MetadataDrivers.registerBookieDriver(bookieDriver.getScheme(), bookieDriver.getClass(),true);
            MetadataBookieDriver driver = MetadataDrivers.getBookieDriver(bookieDriver.getScheme());
            if (!registered) {
                assertEquals(bookieDriver.getClass(), driver.getClass());
            }
        }
        assertTrue(!getBookieDrivers().isEmpty());
    }
}
