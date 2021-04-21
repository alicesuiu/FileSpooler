import alien.config.ConfigUtils;
import alien.io.IOUtils;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
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
    private static Logger logger;
    private static BlockingQueue<FileElement> filesToSend;

   public static void main(String[] args) {
       String sourceDirPath;
       String catalogDirPath;
       Boolean md5Option;
       int maxBackoff, maxNrThreads, i;
       ExecutorService executor;

       logger = ConfigUtils.getLogger(Main.class.getCanonicalName());

       Eos.eosDirPath = System.getProperty("destination.path");
       sourceDirPath = System.getProperty("source.path");
       md5Option = Boolean.parseBoolean(System.getProperty("md5.enable"));
       maxBackoff = Integer.parseInt(System.getProperty("max.backoff"));
       Eos.eosServerPath = System.getProperty("eos.server.path");
       catalogDirPath = System.getProperty("catalog.dir.path");
       maxNrThreads = Integer.parseInt(System.getProperty("max.threads"));

       logger.log(Level.INFO, "EOS Destination Path: " + Eos.eosDirPath);
       logger.log(Level.INFO, "Source Path: " + sourceDirPath);
       logger.log(Level.INFO, "Exponential Backoff Limit: " + maxBackoff);
       logger.log(Level.INFO, "MD5 option: " + md5Option);
       logger.log(Level.INFO, "EOS Server Path: " + Eos.eosServerPath);
       logger.log(Level.INFO, "Catalog Dir Path: " + catalogDirPath);
       logger.log(Level.INFO,"Maximum Number of Threads: " + maxNrThreads);

       executor = Executors.newFixedThreadPool(maxNrThreads);
       filesToSend = addFilesToSend(sourceDirPath);

       FileWatcher watcher = new FileWatcher((new File(sourceDirPath))).addListener(new FileAdapter() {
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

       for (i = 0; i < maxNrThreads; i++)
           executor.submit(new Spooler(filesToSend, maxBackoff, md5Option, catalogDirPath));
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
        long size, ctime;
        UUID guid;
        InputStream inputStream = new FileInputStream(file);
        Properties prop = new Properties();

        prop.load(inputStream);
        surl = prop.getProperty("surl");
        run = prop.getProperty("run");
        metaaccPeriod = prop.getProperty("meta");
        md5 = prop.getProperty("md5", null);
        size = Long.parseLong(prop.getProperty("size"));
        ctime = Long.parseLong(prop.getProperty("ctime"));
        uuid = prop.getProperty("guid", null);
        if (uuid == null) {
            guid = UUID.randomUUID();
        } else {
            guid = UUID.fromString(uuid);
        }

        return new FileElement(md5, surl, size, run, guid, ctime, metaaccPeriod, file.getAbsolutePath());
    }
}
