package spooler;

import alien.config.ConfigUtils;
import alien.se.SE;
import alien.se.SEUtils;
import lazyj.ExtProperties;
import java.io.File;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.LinkedBlockingDeque;
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
    static AtomicInteger nrFilesRegistered = new AtomicInteger(0);
    static AtomicInteger nrFilesFailed = new AtomicInteger(0);
    static AtomicInteger nrFilesRegFailed = new AtomicInteger(0);
    static ExtProperties spoolerProperties;
    static SE eosSE;
    private static Logger logger = ConfigUtils.getLogger(Main.class.getCanonicalName());
    private static BlockingQueue<FileElement> filesToSend = new DelayQueue<>();
    private static BlockingQueue<FileElement> filesToRegister = new LinkedBlockingDeque<>();

    // Default Constants
    private static final String defaultMetadataDir = System.getProperty("user.home") + "/epn2eos";
    static final String defaultCatalogDir = System.getProperty("user.home") + "/daqSpool";
    private static final String defaultEosServer = "ALICE::CERN::EOSALICEO2";
    static final boolean defaultMd5Enable = false;
    static final int defaultMaxBackoff = 10;
    private static final int defaultMaxTransferThreads = 4;
    private static final int defaultMaxRegistrationThreads = 1;

   public static void main(String[] args) {
       spoolerProperties = ConfigUtils.getConfiguration("spooler");
       eosSE = SEUtils.getSE(spoolerProperties.gets("eosServer", defaultEosServer));

       logger.log(Level.INFO, "Metadata Dir Path: " + spoolerProperties.gets("metadataDir", defaultMetadataDir));
       logger.log(Level.INFO, "Exponential Backoff Limit: " + spoolerProperties.geti("maxBackoff", defaultMaxBackoff));
       logger.log(Level.INFO, "MD5 option: " + spoolerProperties.getb("md5Enable", defaultMd5Enable));
       logger.log(Level.INFO, "EOS Server Path: " + eosSE.getName());
       logger.log(Level.INFO, "Catalog Dir Path: " + spoolerProperties.gets("catalogDir", defaultCatalogDir));
       logger.log(Level.INFO, "Maximum Number of Transfer Threads: " + spoolerProperties.geti("maxTransferThreads", defaultMaxTransferThreads));
       logger.log(Level.INFO, "Maximum Number of Registration Threads: " + spoolerProperties.geti("maxRegistrationThreads", defaultMaxRegistrationThreads));

       FileWatcher transferWatcher = new FileWatcher(new File(spoolerProperties.gets("metadataDir", defaultMetadataDir)), filesToSend);
       transferWatcher.watch();

       FileWatcher registrationWatcher = new FileWatcher(new File(spoolerProperties.gets("catalogDir", defaultCatalogDir)), filesToRegister);
       registrationWatcher.watch();

       Thread[] transferThreads = new Thread[spoolerProperties.geti("maxTransferThreads", defaultMaxTransferThreads)];
       for (int i = 0; i < spoolerProperties.geti("maxTransferThreads", defaultMaxTransferThreads); i++) {
           transferThreads[i] = new Thread(new Spooler(filesToSend));
           transferThreads[i].start();
       }

       Thread[] registrationThreads = new Thread[spoolerProperties.geti("maxRegistrationThreads", defaultMaxRegistrationThreads)];
       for (int i = 0; i < spoolerProperties.geti("maxRegistrationThreads", defaultMaxRegistrationThreads); i++) {
           registrationThreads[i] = new Thread(new Registrator(filesToRegister));
           registrationThreads[i].start();
       }
   }
}
