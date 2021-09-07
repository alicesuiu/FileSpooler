package spooler;

import alien.config.ConfigUtils;
import lazyj.ExtProperties;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
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

    private static Logger logger = ConfigUtils.getLogger(Main.class.getCanonicalName());
    private static Map<String, FileExecutor> filesToSend = new HashMap<>();
    private static Map<String, FileExecutor> filesToRegister = new HashMap<>();

    // Default Constants
    private static final String defaultMetadataDir = System.getProperty("user.home") + "/epn2eos";
    static final String defaultCatalogDir = System.getProperty("user.home") + "/daqSpool";
    static final String defaultErrorDir = System.getProperty("user.home") + "/error";
    static final String defaultSEName = "ALICE::CERN::EOSALICEO2";
    static final String defaultseioDaemons = "root://eosaliceo2.cern.ch:1094/";
    static final boolean defaultMd5Enable = false;
    static final int defaultMaxBackoff = 10;
    static final int defaultTransferThreads = 4;
    static final int defaultRegistrationThreads = 1;

   public static void main(String[] args) {
       spoolerProperties = ConfigUtils.getConfiguration("spooler");

       logger.log(Level.INFO, "Metadata Dir Path: " + spoolerProperties.gets("metadataDir", defaultMetadataDir));
       logger.log(Level.INFO, "Exponential Backoff Limit: " + spoolerProperties.geti("maxBackoff", defaultMaxBackoff));
       logger.log(Level.INFO, "MD5 option: " + spoolerProperties.getb("md5Enable", defaultMd5Enable));
       logger.log(Level.INFO, "Catalog Dir Path: " + spoolerProperties.gets("catalogDir", defaultCatalogDir));
       logger.log(Level.INFO, "Error Dir Path: " + spoolerProperties.gets("errorDir", defaultErrorDir));
       logger.log(Level.INFO, "Maximum Number of Transfer Threads: " + spoolerProperties.geti("queue.default.threads", defaultTransferThreads));
       logger.log(Level.INFO, "Maximum Number of Registration Threads: " + spoolerProperties.geti("queue.reg.threads", defaultRegistrationThreads));
       logger.log(Level.INFO, "EOS Server Path: " + spoolerProperties.gets("seName", defaultSEName));
       logger.log(Level.INFO, "EOS seioDaemons: " + spoolerProperties.gets("seioDaemons", defaultseioDaemons));

       FileWatcher transferWatcher = new FileWatcher(new File(spoolerProperties.gets("metadataDir", defaultMetadataDir)), filesToSend, "transfer");
       transferWatcher.watch();

       FileWatcher registrationWatcher = new FileWatcher(new File(spoolerProperties.gets("catalogDir", defaultCatalogDir)), filesToRegister, "register");
       registrationWatcher.watch();

       while (true) {}
   }
}
