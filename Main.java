import alien.config.ConfigUtils;
import alien.io.IOUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.se.SE;
import alien.se.SEUtils;
import alien.catalogue.GUIDUtils;
import lazyj.ExtProperties;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;
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
    public static AtomicInteger nrFilesWatched = new AtomicInteger(0);
    public static ExtProperties spoolerProperties;
    public static SE targetSE;
    private static Logger logger;
    private static BlockingQueue<FileElement> filesToSend;

    // Default Constants
    public static final String defaultMetadataDir = System.getProperty("user.home") + "/epn2eos";
    public static final String defaultCatalogDir = System.getProperty("user.home") + "/daqSpool";
    public static final String defaultEosServer = "ALICE::CERN::EOSALICEO2";
    public static final boolean defaultMd5Enable = false;
    public static final int defaultMaxBackoff = 10;
    public static final int defaultMaxThreads = 4;
    public static final Monitor monitor = MonitorFactory.getMonitor(Main.class.getCanonicalName());

   public static void main(String[] args) {
       ExecutorService executor;

       logger = ConfigUtils.getLogger(Main.class.getCanonicalName());
       spoolerProperties = ConfigUtils.getConfiguration("spooler");
       targetSE = SEUtils.getSE(spoolerProperties.gets("eosServer", defaultEosServer));

       logger.log(Level.INFO, "Metadata Dir Path: " + spoolerProperties.gets("metadataDir", defaultMetadataDir));
       logger.log(Level.INFO, "Exponential Backoff Limit: " + spoolerProperties.geti("maxBackoff", defaultMaxBackoff));
       logger.log(Level.INFO, "MD5 option: " + spoolerProperties.getb("md5Enable", defaultMd5Enable));
       logger.log(Level.INFO, "EOS Server Path: " + targetSE.getName());
       logger.log(Level.INFO, "Catalog Dir Path: " + spoolerProperties.gets("catalogDir", defaultCatalogDir));
       logger.log(Level.INFO, "Maximum Number of Threads: " + spoolerProperties.geti("maxThreads", defaultMaxThreads));

       executor = Executors.newFixedThreadPool(spoolerProperties.geti("maxThreads", defaultMaxThreads));
       filesToSend = addFilesToSend(spoolerProperties.gets("metadataDir", defaultMetadataDir));

       FileWatcher watcher = new FileWatcher((new File(spoolerProperties.gets("metadataDir", defaultMetadataDir))))
           .addListener(new FileAdapter() {
           @Override
           public void onCreated(FileEvent event) {
               File file = event.getFile();
               if (file.getName().endsWith(".done")) {
                   FileElement element;
                   try {
                       element = readMetadata(file);
                       filesToSend.add(element);
                       logger.log(Level.INFO, Thread.currentThread().getName()
                               + " processed a number of " + Main.nrFilesWatched.incrementAndGet() + " files");
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
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            return filesToSend;
        }
    }

    private static String generateURL(String prefix, String period,
      String run, String type, String filename) {

      String url = "";

      url += prefix + "/";
      url += (new Date().getYear() + 1900) + "/";
      url += period.split(":")[1].trim() + "/";
      url += run  + "/";
      url += type;
      url += filename;

      return url;
    }

    private static FileElement readMetadata(File file) throws IOException {
        String surl, run, dataPeriod, md5, uuid, lurl, curl, type;
        long size, ctime, xxhash;
        UUID guid;
        InputStream inputStream = new FileInputStream(file);
        ExtProperties prop = new ExtProperties(inputStream);
        FileWriter writeFile = new FileWriter(file.getAbsolutePath(), true);

        lurl = prop.gets("lurl");
        run = prop.gets("run");
        dataPeriod = prop.gets("dataPeriod");

        type = prop.gets("type", null);
        size = prop.getl("size", 0);
        ctime = prop.getl("ctime", 0);
        uuid = prop.gets("guid", null);

        surl = prop.gets("surl", null);
        md5 = prop.gets("md5", null);
        xxhash = prop.getl("xxHash64", 0);

        if (type == null) {
          type = "raw";
          writeFile.write("type" + ": " + type + "\n");
        }

        if (size == 0) {
          size = Files.size(Paths.get(lurl));
          writeFile.write("size" + ": " + size + "\n");
        }

        if (ctime == 0) {
          ctime = file.lastModified();
          writeFile.write("ctime" + ": " + ctime + "\n");
        }

        if (uuid == null) {
          guid = GUIDUtils.generateTimeUUID();
          writeFile.write("guid" + ": " + guid + "\n");
        }
        else
          guid = UUID.fromString(uuid);

        if (surl == null) {
          surl = generateURL("/eos/test/recv_dir", dataPeriod, run,
            type, lurl.substring(lurl.lastIndexOf('/')));
          writeFile.write("surl" + ": " + surl + "\n");
        }

        curl = generateURL("/alice/data", dataPeriod, run,
          type, lurl.substring(lurl.lastIndexOf('/')));
        writeFile.write("curl" + ": " + curl + "\n");

        writeFile.write("seName" + ": " + Main.targetSE.getName() + "\n");
        writeFile.close();

        return new FileElement(md5, surl, size, run, guid, ctime, dataPeriod,
          file.getAbsolutePath(), xxhash, lurl, type, curl);
    }
}
