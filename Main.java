import alien.config.ConfigUtils;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {
    public static AtomicInteger nrFilesOnSend = new AtomicInteger(0);
    public static AtomicInteger nrFilesSent = new AtomicInteger(0);
    public static AtomicInteger nrFilesFailed = new AtomicInteger(0);
    private static Logger logger;

   public static void main(String[] args) {
       String sourceDirPath;
       String catalogDirPath;
       Boolean md5Option;
       Spooler spooler;
       int maxBackoff;

       logger = ConfigUtils.getLogger(Main.class.getCanonicalName());

       Eos.eosDirPath = System.getProperty("destination.path");
       sourceDirPath = System.getProperty("source.path");
       md5Option = Boolean.parseBoolean(System.getProperty("md5.enable"));
       maxBackoff = Integer.parseInt(System.getProperty("max.backoff"));
       Eos.eosServerPath = System.getProperty("eos.server.path");
       catalogDirPath = System.getProperty("catalog.dir.path");

       logger.log(Level.INFO, "EOS Destination Path: " + Eos.eosDirPath);
       logger.log(Level.INFO, "Source Path: " + sourceDirPath);
       logger.log(Level.INFO, "Exponential Backoff Limit: " + maxBackoff);
       logger.log(Level.INFO, "MD5 option: " + md5Option);
       logger.log(Level.INFO, "EOS Server Path: " + Eos.eosServerPath);
       logger.log(Level.INFO, "Catalog Dir Path: " + catalogDirPath);

       spooler = new Spooler(sourceDirPath, maxBackoff, md5Option, catalogDirPath);
       spooler.run();
   }
}
