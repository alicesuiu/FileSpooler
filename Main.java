import alien.config.ConfigUtils;
import alien.io.IOUtils;
import lazyj.ExtProperties;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {
    public static AtomicInteger nrFilesOnSend = new AtomicInteger(0);
    public static AtomicInteger nrFilesSent = new AtomicInteger(0);
    public static AtomicInteger nrFilesFailed = new AtomicInteger(0);
    public static ExtProperties spoolerProperties;
    private static Logger logger;
    private static BlockingQueue<FileElement> filesToSend;

    // Default Constants
    public static final String defaultSourceDir = System.getProperty("user.dir");
    public static final String defaultDestDir = System.getProperty("user.dir");
    public static final String defaultCatalogDir = System.getProperty("user.dir");
    public static final String defaultEosServer = "root://eos.grid.pub.ro";
    public static final boolean defaultMd5Enable = false;
    public static final int defaultMaxBackoff = 10;
    public static final int defaultMaxThreads = 4;

   public static void main(String[] args) {
       ExecutorService executor;

       logger = ConfigUtils.getLogger(Main.class.getCanonicalName());
       spoolerProperties = ConfigUtils.getConfiguration("spooler");

       logger.log(Level.INFO, "EOS Destination Path: " + spoolerProperties.gets("destinationDir", defaultDestDir));
       logger.log(Level.INFO, "Source Path: " + spoolerProperties.gets("sourceDir", defaultSourceDir));
       logger.log(Level.INFO, "Exponential Backoff Limit: " + spoolerProperties.geti("maxBackoff", defaultMaxBackoff));
       logger.log(Level.INFO, "MD5 option: " + spoolerProperties.getb("md5Enable", defaultMd5Enable));
       logger.log(Level.INFO, "EOS Server Path: " + spoolerProperties.gets("eosServer", defaultEosServer));
       logger.log(Level.INFO, "Catalog Dir Path: " + spoolerProperties.gets("catalogDir", defaultCatalogDir));
       logger.log(Level.INFO, "Maximum Number of Threads: " + spoolerProperties.geti("maxThreads", defaultMaxThreads));

       executor = Executors.newFixedThreadPool(spoolerProperties.geti("maxThreads", defaultMaxThreads));
       filesToSend = addFilesToSend(spoolerProperties.gets("sourceDir", defaultSourceDir));

       FileWatcher watcher = new FileWatcher((new File(spoolerProperties.gets("sourceDir", defaultSourceDir))))
           .addListener(new FileAdapter() {
           @Override
           public void onCreated(FileEvent event) {
               File file = event.getFile();
               if (file.getName().endsWith(".done")) {
                   FileElement element;
                   try {
                       element = readMetadata(file);
                       if (element.getXXHash() == 0) {
                           long xxhash = IOUtils.getXXHash64(element.getFile());
                           element.setXXHash(xxhash);
                       }
                       filesToSend.add(element);
                       logger.log(Level.INFO, "xxHash64 checksum for the file " + element.getFile().getName()
                               + " is " + String.format("%016x", element.getXXHash()));
                   } catch (IOException e) {
                       e.printStackTrace();
                   }
               }
           }
       });
       watcher.watch();

       for (int i = 0; i < spoolerProperties.geti("maxThreads", defaultMaxThreads); i++)
           executor.submit(new Spooler(filesToSend));
   }

   private static BlockingQueue<FileElement> addFilesToSend(String sourceDirPath) {
        try {
            int i;
            long xxhash;
            File dir = new File(sourceDirPath);
            File[] files = dir.listFiles();

            filesToSend = new DelayQueue<>();

            for (i = 0; i < files.length; i++) {
                if (files[i].getName().endsWith(".done"))
                    filesToSend.add(readMetadata(files[i]));
            }

            for (FileElement element : filesToSend) {
                if (element.getXXHash() == 0) {
                    xxhash = IOUtils.getXXHash64(element.getFile());
                    element.setXXHash(xxhash);
                }
                logger.log(Level.INFO, "xxHash64 checksum for the file " + element.getFile().getName()
                        + " is " + String.format("%016x", element.getXXHash()));
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            return filesToSend;
        }
    }

    private static FileElement readMetadata(File file) throws IOException {
        String surl, run, metaaccPeriod, md5, uuid;
        long size, ctime, xxhash;
        UUID guid;
        InputStream inputStream = new FileInputStream(file);
        ExtProperties prop = new ExtProperties(inputStream);

        surl = prop.gets("surl");
        run = prop.gets("run");
        metaaccPeriod = prop.gets("meta");
        md5 = prop.gets("md5", null);
        size = prop.getl("size", 0);
        ctime = prop.getl("ctime", 0);
        uuid = prop.gets("guid", null);
        xxhash = prop.getl("xxHash64", 0);
        if (uuid == null) {
            guid = UUID.randomUUID();
        } else {
            guid = UUID.fromString(uuid);
        }

        return new FileElement(md5, surl, size, run, guid, ctime, metaaccPeriod, file.getAbsolutePath(), xxhash);
    }
}
