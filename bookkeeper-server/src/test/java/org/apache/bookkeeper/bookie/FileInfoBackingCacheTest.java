package org.apache.bookkeeper.bookie;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import org.apache.bookkeeper.bookie.FileInfoBackingCache.CachedFileInfo;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class FileInfoBackingCacheTest {

    final byte[] masterKey = new byte[0];
    final File baseDir;
    final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("backing-cache-test-%d").setDaemon(true).build();
    ExecutorService executor;
  
    public FileInfoBackingCacheTest() throws Exception {
        baseDir = File.createTempFile("foo", "bar");
    }

    @Before
    public void configure() throws Exception {
    	baseDir.delete();
    	baseDir.mkdirs();
        baseDir.deleteOnExit();
        executor = Executors.newCachedThreadPool(threadFactory);
    }

    @After
    public void tearDown() throws Exception {
        if (executor != null) {
            executor.shutdown();
        }
    }
    
    @Test(expected = IOException.class)
    public void testWithoutKey() throws Exception {
        FileInfoBackingCache cache = new FileInfoBackingCache(
                (ledgerId, createIfNotFound) -> {
                    Assert.assertFalse(createIfNotFound);
                    throw new Bookie.NoLedgerException(ledgerId);
                }, FileInfo.CURRENT_HEADER_VERSION);
        cache.loadFileInfo(1, null);
    }
  
    @Test
    public void testBasic() throws Exception {
        FileInfoBackingCache cache = new FileInfoBackingCache(
                (ledgerId, createIfNotFound) -> {
                    File f = new File(baseDir, String.valueOf(ledgerId));
                    f.deleteOnExit();
                    return f;
                }, FileInfo.CURRENT_HEADER_VERSION);
        CachedFileInfo fi = cache.loadFileInfo(1, masterKey);
        Assert.assertEquals(fi.getRefCount(), 1);
        CachedFileInfo fi2 = cache.loadFileInfo(2, masterKey);
        Assert.assertEquals(fi2.getRefCount(), 1);
        CachedFileInfo fi22 = cache.loadFileInfo(2, masterKey);
        Assert.assertEquals(fi22.getRefCount(), 2);
        //check release
        fi.release();
        Assert.assertEquals(fi.getRefCount(), FileInfoBackingCache.DEAD_REF);
        CachedFileInfo fi3 = cache.loadFileInfo(1, null);
        Assert.assertFalse(fi3 == fi);
        Assert.assertEquals(fi3.getRefCount(), 1);
        Assert.assertEquals(fi.getLf(), fi3.getLf());
        //check that close correctly
		cache.closeAllWithoutFlushing();
		Assert.assertEquals(fi.isClosed(), true);
		Assert.assertEquals(fi2.isClosed(), true);
		Assert.assertEquals(fi22.isClosed(), true);
		Assert.assertEquals(fi3.isClosed(), true);
    }
    
    @Test(expected = IOException.class)
    public void testExceptionInCloseAllWithoutFlushing() throws IOException {
        long ledgerid=1;
    	FileInfoBackingCache cache = new FileInfoBackingCache(
                (ledgerId, createIfNotFound) -> {
                    File f = new File(baseDir, String.valueOf(ledgerId));
                    f.deleteOnExit();
                    return f;
                }, FileInfo.CURRENT_HEADER_VERSION);
        CachedFileInfo fi = null;
		fi = cache.loadFileInfo(9, null);
		cache.closeAllWithoutFlushing();
    }
   
    @Test
    public void testErrorEvictingFileInfo()  {
    	final TestAppender appender = new TestAppender();
        final Logger logger = Logger.getRootLogger();
        logger.addAppender(appender);
        long ledgerid=9;
    	FileInfoBackingCache cache = new FileInfoBackingCache(
                (ledgerId, createIfNotFound) -> {
                    File f = new File(baseDir, String.valueOf(ledgerId));
                    f.deleteOnExit();
                    return f;
                }, FileInfo.CURRENT_HEADER_VERSION);
        CachedFileInfo fi = null;
		try {
			fi = cache.loadFileInfo(9, null);
		} catch (IOException e) {
			e.printStackTrace();
		}
		 fi.release();
		 final List<LoggingEvent> log = appender.getLog();
		 final LoggingEvent firstLogEntry = log.get(0);
		 
	//	 assertThat((String) firstLogEntry.getMessage(), is("Error evicting file info("+fi.toString()+") for ledger " +ledgerid +" from backing cache"));	
    }
    
    @Test(expected = IOException.class)
    public void alreadyMarkedDead() throws IOException {
    	 long ledgerid=1;
     	 FileInfoBackingCache cache = new FileInfoBackingCache(
                 (ledgerId, createIfNotFound) -> {
                     File f = new File(baseDir, String.valueOf(ledgerId));
                     f.deleteOnExit();
                     return f;
                 }, FileInfo.CURRENT_HEADER_VERSION);
     	 CachedFileInfo fi = cache.loadFileInfo(9, null);
     	 fi.release();
 		 CachedFileInfo fi2 = cache.loadFileInfo(9, null);
    }
    @Test(expected = IOException.class)
    public void testIsDeleted() throws IOException {
    	 long ledgerid=1;
     	 FileInfoBackingCache cache = new FileInfoBackingCache(
                 (ledgerId, createIfNotFound) -> {
                     File f = new File(baseDir, String.valueOf(ledgerId));
                     f.deleteOnExit();
                     return f;
                 }, FileInfo.CURRENT_HEADER_VERSION);
     	 CachedFileInfo fi = cache.loadFileInfo(9, null);
     	 fi.delete();
 		 CachedFileInfo fi2 = cache.loadFileInfo(9, null);
    }

    @Test 
    public void testRecyclinginConcurrentMode() {
    	      int NTHREDS = 10;
              List<Future<CachedFileInfo>> list = new ArrayList<Future<CachedFileInfo>>();
              ExecutorService executors = Executors.newFixedThreadPool(NTHREDS);
              FileInfoBackingCache cache = new FileInfoBackingCache(
                      (ledgerId, createIfNotFound) -> {
                          File f = new File(baseDir, String.valueOf(ledgerId));
                          f.deleteOnExit();
                          return f;
                      }, FileInfo.CURRENT_HEADER_VERSION);
              for (int i = 0; i < 500; i++) {
                      Callable<CachedFileInfo> worker = new  Callable<CachedFileInfo>() {
                      @Override
                      public CachedFileInfo call() throws Exception {
                    	  CachedFileInfo fi = cache.loadFileInfo(1, masterKey);
                    	  Assert.assertFalse(fi.isClosed());
                    	  fi.release();
						  return fi;
                      }
                  };
                  Future<CachedFileInfo> submit= executors.submit(worker);
                  list.add(submit);
              }
              // This will make the executor accept no new threads
              // and finish all existing threads in the queue
              executors.shutdown();
              // Wait until all threads are finish
              while (!executors.isTerminated()) {
              }
              Set<CachedFileInfo> set = new HashSet<CachedFileInfo>();
              for (Future<CachedFileInfo> future : list) {
                  try {
                      set.add(future.get());
                  } catch (InterruptedException e) {
                      e.printStackTrace();
                  } catch (ExecutionException e) {
                      e.printStackTrace();
                  }
              }
              for (CachedFileInfo fi: set) {
                      Assert.assertTrue(fi.isClosed());
                      Assert.assertEquals(FileInfoBackingCache.DEAD_REF, fi.getRefCount());
                  }
              }

class TestAppender extends AppenderSkeleton {
    private final List<LoggingEvent> log = new ArrayList<LoggingEvent>();

    @Override
    public boolean requiresLayout() {
        return false;
    }

    @Override
    protected void append(final LoggingEvent loggingEvent) {
        log.add(loggingEvent);
    }

    @Override
    public void close() {
    }

    public List<LoggingEvent> getLog() {
        return new ArrayList<LoggingEvent>(log);
    }
}
}