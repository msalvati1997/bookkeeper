package org.apache.bookkeeper.meta;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.net.URI;
import java.util.Collection;

import static org.apache.bookkeeper.meta.MetadataDrivers.ZK_BOOKIE_DRIVER_CLASS;
import static org.apache.bookkeeper.meta.MetadataDrivers.ZK_CLIENT_DRIVER_CLASS;
import static org.junit.Assert.assertEquals;

/**
 * Unit test of @link MetadataDrivers
 * Method tested : getClientDriver/getBookieDriver
 * Martina Salvati
 */

@RunWith(Enclosed.class)
public class GetClientAndBookieDriverTest {


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
					{"Zk", null},
					{"zK", null},
					{"ZK", null},
					{((String) null), NullPointerException.class}, //adequacy
					{((URI) null), NullPointerException.class},  //adequacy
					{"unknown", IllegalArgumentException.class}, //adequacy
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
					{(String) null, NullPointerException.class},
					{(URI) null, NullPointerException.class},
					{("//127.0.0.1/ledgers"), NullPointerException.class},
				    {"hierarchical://127.0.0.1/ledgers", IllegalArgumentException.class},
					{"", NullPointerException.class}, //adequacy
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
}

