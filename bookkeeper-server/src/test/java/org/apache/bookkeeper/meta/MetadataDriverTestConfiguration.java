package org.apache.bookkeeper.meta;

public class MetadataDriverTestConfiguration {
	
	  //configurazione del clientdriver1/clientdriver2 e bookieserver1 e bookieserver2
	   static class ClientDriver1 extends ClientDriverMock {

	        @Override
	        public String getScheme() {
	            return "driver1";
	        }

	    }

	    static class ClientDriver2 extends ClientDriverMock {

	        @Override
	        public String getScheme() {
	            return "driver2";
	        }

	    }


	    static class BookieDriver1 extends BookieDriverMock {

	        @Override
	        public String getScheme() {
	            return "driver1";
	        }

	    }

	    static class BookieDriver2 extends BookieDriverMock {

	        @Override
	        public String getScheme() {
	            return "driver2";
	        }

	    }

}
