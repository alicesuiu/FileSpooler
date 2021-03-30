import java.awt.desktop.SystemEventListener;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {
    public static AtomicInteger nrFilesOnSend = new AtomicInteger(0);
    public static AtomicInteger nrFilesSent = new AtomicInteger(0);
    public static AtomicInteger nrFilesFailed = new AtomicInteger(0);

   public static void main(String[] args) {
       String eosDirPath;
       String eosServerPath;
       String sourceDirPath;
       Boolean md5Option;
       Spooler spooler;
       int maxBackoff;

       eosDirPath = System.getProperty("destination.path");
       sourceDirPath = System.getProperty("source.path");
       md5Option = Boolean.parseBoolean(System.getProperty("md5.enable"));
       maxBackoff = Integer.parseInt(System.getProperty("max.backoff"));
       eosServerPath = System.getProperty("eos.server.path");

       System.out.println("EOS Destination Path: " + eosDirPath);
       System.out.println("Source Path: " + sourceDirPath);
       System.out.println("Exponential Backoff Limit: " + maxBackoff);
       System.out.println("MD5 option: " + md5Option);
       System.out.println("EOS Server Path: " + eosServerPath);

       spooler = new Spooler(eosServerPath, eosDirPath, sourceDirPath, maxBackoff, md5Option);
       System.out.println(spooler.getFilesToSend());

       spooler.run();
   }
}
