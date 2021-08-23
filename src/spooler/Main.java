package spooler;

import alien.config.ConfigUtils;
import alien.se.SE;
import alien.se.SEUtils;
import lazyj.ExtProperties;
import java.io.File;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author asuiu
 * @since March 30, 2021
 */
public class Main {
    static AtomicInteger nrFilesOnSend = new AtomicInteger(0);
    static AtomicInteger nrFilesSent = new AtomicInteger(0);
    static AtomicInteger nrFilesFailed = new AtomicInteger(0);
    static ExtProperties spoolerProperties;
    static SE targetSE;
    private static Logger logger = ConfigUtils.getLogger(Main.class.getCanonicalName());
    private static BlockingQueue<FileElement> filesToSend = new DelayQueue<>();

    // Default Constants
    static final String defaultMetadataDir = System.getProperty("user.home") + "/epn2eos";
    static final String defaultCatalogDir = System.getProperty("user.home") + "/daqSpool";
    private static final String defaultEosServer = "ALICE::CERN::EOSALICEO2";
    static final boolean defaultMd5Enable = false;
    static final int defaultMaxBackoff = 10;
    private static final int defaultMaxThreads = 4;

   public static void main(String[] args) {
       spoolerProperties = ConfigUtils.getConfiguration("spooler");
       targetSE = SEUtils.getSE(spoolerProperties.gets("eosServer", defaultEosServer));

       logger.log(Level.INFO, "Metadata Dir Path: " + spoolerProperties.gets("metadataDir", defaultMetadataDir));
       logger.log(Level.INFO, "Exponential Backoff Limit: " + spoolerProperties.geti("maxBackoff", defaultMaxBackoff));
       logger.log(Level.INFO, "MD5 option: " + spoolerProperties.getb("md5Enable", defaultMd5Enable));
       logger.log(Level.INFO, "EOS Server Path: " + targetSE.getName());
       logger.log(Level.INFO, "Catalog Dir Path: " + spoolerProperties.gets("catalogDir", defaultCatalogDir));
       logger.log(Level.INFO, "Maximum Number of Threads: " + spoolerProperties.geti("maxThreads", defaultMaxThreads));

       FileWatcher watcher = new FileWatcher(new File(spoolerProperties.gets("metadataDir", defaultMetadataDir)), filesToSend);
       watcher.watch();

       Thread[] threads = new Thread[spoolerProperties.geti("maxThreads", defaultMaxThreads)];
       for (int i = 0; i < spoolerProperties.geti("maxThreads", defaultMaxThreads); i++) {
           threads[i] = new Thread(new Spooler(filesToSend));
            threads[i].start();
       }
   }
}
