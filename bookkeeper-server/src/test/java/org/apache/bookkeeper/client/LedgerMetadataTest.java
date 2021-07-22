package org.apache.bookkeeper.client;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.runners.Parameterized;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.experimental.runners.Enclosed;

/**
 * Unit test for {@link LedgerMetadataBuilder}.
 * 
 * @author Martina Salvati
 */


@RunWith(Enclosed.class)
public class LedgerMetadataTest  {
	
	@RunWith(Parameterized.class)
	public static class TestMetadataBuilderCreate {
	    private static List<BookieId> ensemble;
		private final  Object digestType;
	    private final byte[] passwd ;
	    private final long ledgerId;
	    private final int ensembleSize;
	    private final int writequorumSize;
	    private final int ackQuorumSize;
	    private final int version;
	    private final long ctime;
	    private final long ctoken;
	    private final Map<String, byte[]> customMetadata;
	    private final long lastEntryId ;
	    private final long firstEntryId ;
	    private final boolean storeCtime;
	    private final long lenght;

     public TestMetadataBuilderCreate(Object digestType,  byte[] passwd, long ledgerId,
			int ensembleSize, int writequorumSize, int ackQuorumSize, int version, long ctime, long ctoken,
			Map<String, byte[]> customMetadata, long lastEntryId, long firstEntryId, boolean storeCtime, long lenght) {
		super();
		this.digestType = digestType;
		this.passwd = passwd;
		this.ledgerId = ledgerId;
		this.ensembleSize = ensembleSize;
		this.writequorumSize = writequorumSize;
		this.ackQuorumSize = ackQuorumSize;
		this.version = version;
		this.ctime = ctime;
		this.ctoken = ctoken;
		this.customMetadata = customMetadata;
		this.lastEntryId = lastEntryId;
		this.firstEntryId = firstEntryId;
		this.storeCtime = storeCtime;
		this.lenght = lenght;
	}
/*
 *  digestType: {CRC32},{MAC},{CRC32C},{DUMMY}
 *  passwd: {byte[]}, {null}
    ledgerId:  {>0}, {=0}
    ensembleSize: {>0}, {=0}
    WritequorumSize: {=ensembleSize},{<ensembleSize}
    AckQuorumSize : {=WriteQuosumSize},{<WriteQuorumSize}
    version : {>=1} , {<=10}
    ctime :{=0},{>0}
    ctoken : {=0},{>0}
    customMetadata : {Map<String,byte[]>}, {Collection.emptyMap}
    lastEntryId : {=firstEntryId}, {>firstEntryId}
    firstEntryId : {<0},{=0},{>0}
    storeCtime : {true}, {false}
    lenght: {=0},{>0}
*/

	@Parameterized.Parameters(name = "TestLedgerMetadataTest: {0},{1},{2},{3},{4},{5}")
	   public static Collection data() {
	      return java.util.Arrays.asList(new Object[][] {
	    	  {org.apache.bookkeeper.client.api.DigestType.MAC,"psw".getBytes(),0,3,2,1,1,1,1,Collections.emptyMap(),0,0,true,0} ,
	    	  {org.apache.bookkeeper.client.api.DigestType.DUMMY,null,0,0,0,0,2,1,0,Collections.emptyMap(),0,0,false,0} ,
	    	  {org.apache.bookkeeper.client.api.DigestType.CRC32C,null,123L,2,2,0,0,1,0,generateRandomMap(2),0,0,false,0} ,
	    	  {org.apache.bookkeeper.client.api.DigestType.CRC32,null,123L,2,1,1,0,1,0,generateRandomMap(2),0,0,false,0} ,	 
	      });
	   }
	
	
	private static Map<String,byte[]> generateRandomMap(int size) {
		Map<String,byte[]> map = new HashMap<String,byte[]>();
		for(int i=0;i<size;i++) {
		   String key = "key"+i;
		   map.put(key,key.getBytes());
	}
		return map;
}
	@Test
    public void testNonclosedLedgerMetadata() {
		 LedgerMetadata metadata = getMetadataNonClosed( ledgerId, ensembleSize, writequorumSize, ackQuorumSize, version,passwd,  ctime, ctoken, customMetadata, storeCtime,lenght,lastEntryId,firstEntryId);
    	 assertEquals(ledgerId, metadata.getLedgerId());
         assertEquals(ensembleSize, metadata.getEnsembleSize());
         assertEquals(writequorumSize, metadata.getWriteQuorumSize());
         assertEquals(ackQuorumSize, metadata.getAckQuorumSize());
         if(passwd!=null) {
             assertEquals(digestType, metadata.getDigestType()); }       
         assertEquals(customMetadata, metadata.getCustomMetadata());
         assertEquals(ctime, metadata.getCtime());
         assertEquals(-1L, metadata.getLastEntryId());
         assertEquals(0, metadata.getLength());
         assertFalse(metadata.isClosed());
         assertEquals(1, metadata.getAllEnsembles().size());
         assertEquals(ctoken, metadata.getCToken());
		 assertEquals(ensemble, metadata.getAllEnsembles().get(firstEntryId));
         assertEquals(ensemble, metadata.getEnsembleAt(99L));
         assertEquals(customMetadata, metadata.getCustomMetadata());
         assertEquals(version, metadata.getMetadataFormatVersion());
    }
	@Test
    public void testclosedLedgerMetadata() {
		 LedgerMetadata metadata = getMetadataClosed( ledgerId, ensembleSize, writequorumSize, ackQuorumSize, version,passwd,  ctime, ctoken, customMetadata, storeCtime,lenght,lastEntryId,firstEntryId);
    	 assertEquals(ledgerId, metadata.getLedgerId());
         assertEquals(ensembleSize, metadata.getEnsembleSize());
         assertEquals(writequorumSize, metadata.getWriteQuorumSize());
         assertEquals(ackQuorumSize, metadata.getAckQuorumSize());
         if(passwd!=null) {
         assertEquals(digestType, metadata.getDigestType()); }
         assertEquals(ctime, metadata.getCtime());
         assertEquals(lastEntryId, metadata.getLastEntryId());
         assertEquals(lenght, metadata.getLength());
         assertTrue(metadata.isClosed());
         assertEquals(1, metadata.getAllEnsembles().size());
         assertEquals(lastEntryId, metadata.getLastEntryId());
         assertEquals(lenght, metadata.getLength());
         assertEquals(ctoken, metadata.getCToken());
		 assertEquals(ensemble, metadata.getAllEnsembles().get(firstEntryId));
         assertEquals(ensemble, metadata.getEnsembleAt(99L));
         assertEquals(customMetadata, metadata.getCustomMetadata());
         assertEquals(version, metadata.getMetadataFormatVersion());
    }
	@Test
    public void testFromOtherLedgersNonClosed() {	
		 LedgerMetadata metadata = getMetadataFromOtherNonClosed( ledgerId, ensembleSize, writequorumSize, ackQuorumSize, version,passwd,  ctime, ctoken, customMetadata, storeCtime, lenght,lastEntryId,firstEntryId);
		 assertEquals(ledgerId, metadata.getLedgerId());
         assertEquals(ensembleSize, metadata.getEnsembleSize());
         assertEquals(writequorumSize, metadata.getWriteQuorumSize());
         assertEquals(ackQuorumSize, metadata.getAckQuorumSize());
         if(passwd!=null) {
             assertEquals(digestType, metadata.getDigestType()); }         
         assertEquals(customMetadata, metadata.getCustomMetadata());
         assertEquals(ctime, metadata.getCtime());
         assertEquals(-1L, metadata.getLastEntryId());
         assertEquals(0, metadata.getLength());
         assertFalse(metadata.isClosed());
         assertEquals(1, metadata.getAllEnsembles().size());
         assertEquals(0, metadata.getCToken());
		 assertEquals(ensemble, metadata.getAllEnsembles().get(firstEntryId));
         assertEquals(ensemble, metadata.getEnsembleAt(99L));
         assertEquals(customMetadata, metadata.getCustomMetadata());
         assertEquals(version, metadata.getMetadataFormatVersion());
    }
	@Test
    public void testFromOtherLedgersClosed() {	
		 LedgerMetadata metadata = getMetadataFromOtherClosed( ledgerId, ensembleSize, writequorumSize, ackQuorumSize, version,passwd,  ctime, ctoken, customMetadata, storeCtime,lenght,lastEntryId,firstEntryId);
		 assertEquals(ledgerId, metadata.getLedgerId());
         assertEquals(ensembleSize, metadata.getEnsembleSize());
         assertEquals(writequorumSize, metadata.getWriteQuorumSize());
         assertEquals(ackQuorumSize, metadata.getAckQuorumSize());
         if(passwd!=null) {
             assertEquals(digestType, metadata.getDigestType()); }         
         assertEquals(ctime, metadata.getCtime());
         assertEquals(lastEntryId, metadata.getLastEntryId());
         assertEquals(lenght, metadata.getLength());
         assertTrue(metadata.isClosed());
         assertEquals(1, metadata.getAllEnsembles().size());
         assertEquals(lastEntryId, metadata.getLastEntryId());
         assertEquals(lenght, metadata.getLength());
         assertEquals(0, metadata.getCToken());
		 assertEquals(ensemble, metadata.getAllEnsembles().get(firstEntryId));
         assertEquals(ensemble, metadata.getEnsembleAt(99L));
         assertEquals(customMetadata, metadata.getCustomMetadata());
         assertEquals(version, metadata.getMetadataFormatVersion());
    }

	public static LedgerMetadata getMetadataNonClosed(long ledgerId,int ensembleSize,int WritequorumSize,int AckQuorumSize,int version, byte[] passwd,long ctime,long ctoken, Map<String, byte[]> customMetadata,  boolean storeCtime, long lenght, long lastEntryId, long firstEntryId) {
		ensemble= new ArrayList<>();
		for(int i=0;i<ensembleSize;i++) {
			ensemble.add(new BookieSocketAddress("192.0.2.1", 1234).toBookieId());
		}
		if(passwd!=null) {
		org.apache.bookkeeper.client.api.LedgerMetadata metadata = LedgerMetadataBuilder.create()
	            .withEnsembleSize(ensemble.size()).withWriteQuorumSize(WritequorumSize).withAckQuorumSize(AckQuorumSize)
	            .withDigestType((org.apache.bookkeeper.client.api.DigestType.MAC)).withPassword(passwd)
	            .newEnsembleEntry(firstEntryId, ensemble)
	            .withId(ledgerId)
	            .withMetadataFormatVersion(version)
	            .withClosedState()
	            .withCreationTime(ctime)
	            .withCToken(ctoken)
	            .withInRecoveryState()
	            .withCustomMetadata(customMetadata)
	            .storingCreationTime(storeCtime )
	            .build();
		        return metadata;
		 }
		else {
			org.apache.bookkeeper.client.api.LedgerMetadata metadata = LedgerMetadataBuilder.create()
		            .withEnsembleSize(ensemble.size()).withWriteQuorumSize(WritequorumSize).withAckQuorumSize(AckQuorumSize)
		            .newEnsembleEntry(firstEntryId, ensemble)
		            .withId(ledgerId)
		            .withMetadataFormatVersion(version)
		            .withClosedState()
		            .withCreationTime(ctime)
		            .withCToken(ctoken)
		            .withInRecoveryState()
		            .withCustomMetadata(customMetadata)
		            .storingCreationTime(storeCtime )
		            .build();
			        return metadata;
		}
	}
	public static LedgerMetadata getMetadataFromOtherNonClosed(long ledgerId,int ensembleSize,int WritequorumSize,int AckQuorumSize,int version, byte[] passwd,long ctime,long ctoken, Map<String, byte[]> customMetadata,  boolean storeCtime, long lenght, long lastEntryId,long firstentry) {
		ensemble= new ArrayList<>();
		for(int i=0;i<ensembleSize;i++) {
			ensemble.add(new BookieSocketAddress("192.0.2.1", 1234).toBookieId());
		}
		if(passwd!=null) {
org.apache.bookkeeper.client.api.LedgerMetadata other = LedgerMetadataBuilder.create()
        .withEnsembleSize(ensemble.size()).withWriteQuorumSize(WritequorumSize).withAckQuorumSize(AckQuorumSize)
        .withDigestType(org.apache.bookkeeper.client.api.DigestType.MAC).withPassword(passwd)
        .newEnsembleEntry(firstentry, ensemble)
        .withId(ledgerId)
        .withMetadataFormatVersion(version)
        .withClosedState()
        .withCreationTime(ctime)
        .withCToken(ctoken)
        .withInRecoveryState()
        .withCustomMetadata(customMetadata)
        .storingCreationTime(storeCtime )
        .build();

org.apache.bookkeeper.client.api.LedgerMetadata metadata = LedgerMetadataBuilder.from(other).build();
 return metadata;
 } else {
	 org.apache.bookkeeper.client.api.LedgerMetadata other = LedgerMetadataBuilder.create()
		        .withEnsembleSize(ensembleSize).withWriteQuorumSize(WritequorumSize).withAckQuorumSize(AckQuorumSize)
		        .newEnsembleEntry(firstentry, ensemble)
		        .withId(ledgerId)
		        .withMetadataFormatVersion(version)
		        .withClosedState()
		        .withCreationTime(ctime)
		        .withCToken(ctoken)
		        .withInRecoveryState()
		        .withCustomMetadata(customMetadata)
		        .storingCreationTime(storeCtime )
		        .build();
	 org.apache.bookkeeper.client.api.LedgerMetadata metadata = LedgerMetadataBuilder.from(other).build();
	 return metadata;

 }}
    public static LedgerMetadata getMetadataClosed(long ledgerId,int ensembleSize,int WritequorumSize,int AckQuorumSize,int version, byte[] passwd,long ctime,long ctoken, Map<String, byte[]> customMetadata,  boolean storeCtime, long lenght, long lastEntryId,long firstentry) {
    	ensemble= new ArrayList<>();
		for(int i=0;i<ensembleSize;i++) {
			ensemble.add(new BookieSocketAddress("192.0.2.1", 1234).toBookieId());
		}
		if(passwd!=null) {
org.apache.bookkeeper.client.api.LedgerMetadata metadata = LedgerMetadataBuilder.create()
        .withEnsembleSize(ensembleSize).withWriteQuorumSize(WritequorumSize).withAckQuorumSize(AckQuorumSize)
        .withDigestType((org.apache.bookkeeper.client.api.DigestType.MAC)).withPassword(passwd)
        .newEnsembleEntry(firstentry, ensemble)
        .withId(ledgerId)
        .withMetadataFormatVersion(version)
        .withCreationTime(ctime)
        .withCToken(ctoken)
        .withInRecoveryState()
        .withCustomMetadata(customMetadata)
        .storingCreationTime(storeCtime)
        .withLength(lenght)
        .withLastEntryId(lastEntryId)
        .replaceEnsembleEntry(firstentry, ensemble)
        .withClosedState()
        .build();
 return metadata;
 } else {
	 org.apache.bookkeeper.client.api.LedgerMetadata metadata = LedgerMetadataBuilder.create()
		        .withEnsembleSize(ensembleSize).withWriteQuorumSize(WritequorumSize).withAckQuorumSize(AckQuorumSize)
		        .newEnsembleEntry(firstentry, ensemble)
		        .withId(ledgerId)
		        .withMetadataFormatVersion(version)
		        .withCreationTime(ctime)
		        .withCToken(ctoken)
		        .withInRecoveryState()
		        .withCustomMetadata(customMetadata)
		        .storingCreationTime(storeCtime)
		        .withLength(lenght)
		        .withLastEntryId(lastEntryId)
		        .replaceEnsembleEntry(firstentry, ensemble)
		        .withClosedState()
		        .build();
		 return metadata;
 }
 }
    public static LedgerMetadata getMetadataFromOtherClosed(long ledgerId,int ensembleSize,int WritequorumSize,int AckQuorumSize,int version, byte[] passwd,long ctime,long ctoken, Map<String, byte[]> customMetadata,  boolean storeCtime, long lenght, long lastEntryId,long firstentry) {
    	
    	ensemble= new ArrayList<>();
		for(int i=0;i<ensembleSize;i++) {
			ensemble.add(new BookieSocketAddress("192.0.2.1", 1234).toBookieId());
		}
		if(passwd!=null) {
org.apache.bookkeeper.client.api.LedgerMetadata other = LedgerMetadataBuilder.create()
        .withEnsembleSize(ensembleSize).withWriteQuorumSize(WritequorumSize).withAckQuorumSize(AckQuorumSize)
        .withDigestType((org.apache.bookkeeper.client.api.DigestType.MAC)).withPassword(passwd)
        .newEnsembleEntry(firstentry, ensemble)
        .withId(ledgerId)
        .withMetadataFormatVersion(version)
        .withCreationTime(ctime)
        .withCToken(ctoken)
        .withInRecoveryState()
        .withCustomMetadata(customMetadata)
        .storingCreationTime(storeCtime)
        .withLength(lenght)
        .withLastEntryId(lastEntryId)
        .replaceEnsembleEntry(firstentry, ensemble)
        .withClosedState()
        .build();
org.apache.bookkeeper.client.api.LedgerMetadata metadata = LedgerMetadataBuilder.from(other).build();

 return metadata;
 } else {
	 org.apache.bookkeeper.client.api.LedgerMetadata other = LedgerMetadataBuilder.create()
		        .withEnsembleSize(ensembleSize).withWriteQuorumSize(WritequorumSize).withAckQuorumSize(AckQuorumSize)
		        .newEnsembleEntry(firstentry, ensemble)
		        .withId(ledgerId)
		        .withMetadataFormatVersion(version)
		        .withCreationTime(ctime)
		        .withCToken(ctoken)
		        .withInRecoveryState()
		        .withCustomMetadata(customMetadata)
		        .storingCreationTime(storeCtime)
		        .withLength(lenght)
		        .withLastEntryId(lastEntryId)
		        .replaceEnsembleEntry(firstentry, ensemble)
		        .withClosedState()
		        .build();
		org.apache.bookkeeper.client.api.LedgerMetadata metadata = LedgerMetadataBuilder.from(other).build();
		 return metadata;
 }
 }
	    	
    } 

	 public static class TestMetadataBuilderException  {
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
					List <BookieId> ensemble_passed = new ArrayList<BookieId>();
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

		 //New entry must have a first entry greater than any existing ensemble key 2
		 @Test(expected = IllegalArgumentException.class)
		 public void testSetEnsembleSize2() {

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
					 .newEnsembleEntry(0, ensemble)
					 .withEnsembleSize(0)
					 .build();
			 LedgerMetadataBuilder metadata2 = LedgerMetadataBuilder.from(metadata);
			 metadata2.newEnsembleEntry(-1, ensemble);
		 }

		 //New entry must have a first entry greater than any existing ensemble key 2
		 @Test(expected = IllegalArgumentException.class)
		 public void testSetEnsembleSize3() {

			 ensemble= new ArrayList<BookieId>();
			 int ensembleSize=2;
			 for(int i=0;i<ensembleSize;i++) {
				 ensemble.add(new BookieSocketAddress("192.0.2.1", 1234).toBookieId());
			 }
			 org.apache.bookkeeper.client.api.LedgerMetadata metadata = LedgerMetadataBuilder.create()
					 .withWriteQuorumSize(0)
					 .withAckQuorumSize(0)
					 .withId(0)
					 .withEnsembleSize(2)
					 .newEnsembleEntry(1 ,ensemble)
					 .build();
			 LedgerMetadataBuilder metadata2 = LedgerMetadataBuilder.from(metadata);
			 metadata2.newEnsembleEntry(0, ensemble);
		 }
	 }
			   }
			      
				




