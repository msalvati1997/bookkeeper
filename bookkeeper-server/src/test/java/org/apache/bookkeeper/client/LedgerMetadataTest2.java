package org.apache.bookkeeper.client;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.meta.LedgerIdGenerator;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieClient;
import org.eclipse.jetty.http.MetaData;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;
import org.junit.Before;
import org.junit.Rule;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import java.io.IOException;
import java.util.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class LedgerMetadataTest2 {
	
    private static List<BookieId> ensemble;
    private static long ctime = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
    private static long ctoken = 123456L;
    private final Map<String, byte[]> customMetadata=Collections.emptyMap();
    boolean storeCtime=true;   
   

	   //Ledger id must be set
		@Test(expected = IllegalArgumentException.class)
		public void  withtouId() {
			org.apache.bookkeeper.client.api.LedgerMetadata metadata = LedgerMetadataBuilder.create()
						.withEnsembleSize(ensemble.size()).withWriteQuorumSize(2).withAckQuorumSize(1)
				        .newEnsembleEntry(-1, ensemble)
				        .withMetadataFormatVersion(org.apache.bookkeeper.meta.LedgerMetadataSerDe.METADATA_FORMAT_VERSION_1)
				        .withClosedState()
				        .withCreationTime(ctime)
				        .withCToken(ctoken)
				        .withInRecoveryState()
				        .withCustomMetadata(customMetadata)
				        .storingCreationTime(storeCtime)
						.build();
		}
		@Test(expected = IllegalArgumentException.class)
		//There must be at least one ensemble in the ledger
		public void  withtouEnsembleSize() {
			org.apache.bookkeeper.client.api.LedgerMetadata metadata = LedgerMetadataBuilder.create()		
				        .withMetadataFormatVersion(org.apache.bookkeeper.meta.LedgerMetadataSerDe.METADATA_FORMAT_VERSION_1)
				        .withClosedState()
			            .withId(100L)
				        .withCreationTime(ctime)
				        .withCToken(ctoken)
				        .withInRecoveryState()
				        .withCustomMetadata(customMetadata)
				        .storingCreationTime(storeCtime )
						.build();
		}
		//Write quorum must be less or equal to ensemble size
		@Test (expected = IllegalArgumentException.class)
		public void  illegalEnsamble1() {
			org.apache.bookkeeper.client.api.LedgerMetadata metadata = LedgerMetadataBuilder.create()
		            .withEnsembleSize(0)
		            .withWriteQuorumSize(1)
		            .withAckQuorumSize(3)
		            .withId(100L)
		            .newEnsembleEntry(-1,  new ArrayList<>())
		            .withMetadataFormatVersion(org.apache.bookkeeper.meta.LedgerMetadataSerDe.METADATA_FORMAT_VERSION_1)
		            .withClosedState()
		            .withCreationTime(ctime)
		            .withCToken(ctoken)
		            .withInRecoveryState()
		            .withCustomMetadata(customMetadata)
		            .storingCreationTime(storeCtime )
		            .build();
		}
		//Size of passed in ensemble must match the ensembleSize of the builder
		@Test (expected = IllegalArgumentException.class)
		public void  illegalEnsamble2() {
			ensemble= new ArrayList<BookieId>();
			for(int i=0;i<5;i++) {
				ensemble.add(new BookieSocketAddress("192.0.2.1", 1234).toBookieId());
			}
			org.apache.bookkeeper.client.api.LedgerMetadata metadata = LedgerMetadataBuilder.create()
		            .withEnsembleSize(ensemble.size())
		            .withWriteQuorumSize(1)
		            .withAckQuorumSize(3)
		            .withId(100L)
		            .newEnsembleEntry(-1,  new ArrayList<>())
		            .withMetadataFormatVersion(org.apache.bookkeeper.meta.LedgerMetadataSerDe.METADATA_FORMAT_VERSION_1)
		            .withClosedState()
		            .withCreationTime(ctime)
		            .withCToken(ctoken)
		            .withInRecoveryState()
		            .withCustomMetadata(customMetadata)
		            .storingCreationTime(storeCtime )
		            .build();
			 assertTrue("Size of passed in ensemble must match the ensembleSize of the builder",
					 metadata.toString().contains(Base64.getEncoder().encodeToString(String.valueOf(ensemble.size()).getBytes())));
		}
		//Write quorum must be greater or equal to ack quorum
		@Test(expected = IllegalArgumentException.class)
		public void  illegalAckQuorum() {
			ensemble= new ArrayList<BookieId>();
			int ensembleSize=2;
			for(int i=0;i<ensembleSize;i++) {
				ensemble.add(new BookieSocketAddress("192.0.2.1", 1234).toBookieId());
			}
			org.apache.bookkeeper.client.api.LedgerMetadata metadata = LedgerMetadataBuilder.create()
		            .withEnsembleSize(ensemble.size()).withWriteQuorumSize(0).withAckQuorumSize(1)
		            .newEnsembleEntry(0, ensemble)
		            .withId(100L)
		            .withMetadataFormatVersion(org.apache.bookkeeper.meta.LedgerMetadataSerDe.METADATA_FORMAT_VERSION_1)
		            .withClosedState()
		            .withCreationTime(ctime)
		            .withCToken(ctoken)
		            .withInRecoveryState()
		            .withCustomMetadata(customMetadata)
		            .storingCreationTime(storeCtime )
		            .build();
		}
		
		//Size of passed in ensemble must match the ensembleSize of the builder
		@Test(expected = IllegalArgumentException.class)
		public void passedEnsembleIllegal() {
			ensemble= new ArrayList<BookieId>();
			int ensembleSize=2;
			for(int i=0;i<ensembleSize;i++) {
				ensemble.add(new BookieSocketAddress("192.0.2.1", 1234).toBookieId());
			}
			ArrayList ensemble_passed = new ArrayList<>();
			for(int i=0;i<30;i++) {
				ensemble.add(new BookieSocketAddress("192.0.2.1", 1234).toBookieId());
			}
			org.apache.bookkeeper.client.api.LedgerMetadata metadata = LedgerMetadataBuilder.create()
		            .withEnsembleSize(ensemble.size()).withWriteQuorumSize(2).withAckQuorumSize(2)
		            .newEnsembleEntry(0, ensemble)
		            .withId(100L)
		            .withMetadataFormatVersion(org.apache.bookkeeper.meta.LedgerMetadataSerDe.METADATA_FORMAT_VERSION_1)
		            .withClosedState()
		            .withCreationTime(ctime)
		            .withCToken(ctoken)
		            .replaceEnsembleEntry(0, ensemble_passed)
		            .withInRecoveryState()
		            .withCustomMetadata(customMetadata)
		            .storingCreationTime(storeCtime )
		            .build();
		}
		//New entry must have a first entry greater than any existing ensemble key
		@Test(expected = IllegalArgumentException.class)
		public void testSetEnsembleSize() {
		 
			ensemble= new ArrayList<BookieId>();
			int ensembleSize=2;
			for(int i=0;i<ensembleSize;i++) {
				ensemble.add(new BookieSocketAddress("192.0.2.1", 1234).toBookieId());
			}
			org.apache.bookkeeper.client.api.LedgerMetadata metadata = LedgerMetadataBuilder.create()
		            .withWriteQuorumSize(0)
		            .withAckQuorumSize(0)
		            .withId(0)
		            .withEnsembleSize(ensemble.size())
		            .newEnsembleEntry(0, ensemble)
		            .build();
		   LedgerMetadataBuilder metadata2 = LedgerMetadataBuilder.from(metadata);
		   metadata2.newEnsembleEntry(-1, ensemble);
		}
		//Can only set ensemble size before adding ensembles to the builder
		@Test(expected = IllegalStateException.class)
		public void testEnsembleSize() {
			ensemble= new ArrayList<BookieId>();
			int ensembleSize=2;
			for(int i=0;i<ensembleSize;i++) {
				ensemble.add(new BookieSocketAddress("192.0.2.1", 1234).toBookieId());
			}
			org.apache.bookkeeper.client.api.LedgerMetadata metadata = LedgerMetadataBuilder.create()
		            .withWriteQuorumSize(0)
		            .withAckQuorumSize(0)
		            .withId(0)
		            .withEnsembleSize(0)
		            .newEnsembleEntry(0, new ArrayList<BookieId>())
		            .withEnsembleSize(0)
		            .build();
		}
	   }
	      
		
   
