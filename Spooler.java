import alien.config.ConfigUtils;
import alien.io.IOUtils;
import net.jpountz.xxhash.StreamingXXHash64;
import net.jpountz.xxhash.XXHashFactory;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Spooler {
    private BlockingQueue<FileElement> filesToSend;
    private List<FileElement> md5Checksums;
    private final String sourceDirPath;
    private final String catalogDirPath;
    private final Boolean md5Option;
    private final int maxBackoff;
    private final Logger logger;

    // Constants
    private final int badTransfer = 1;
    private final int successfulTransfer = 0;

    public Spooler(String sourceDirPath, int maxBackoff, Boolean md5Option, String catalogDirPath) {
        this.sourceDirPath = sourceDirPath;
        this.catalogDirPath = catalogDirPath;
        this.maxBackoff = maxBackoff;
        this.md5Option = md5Option;
        md5Checksums = new ArrayList<>();
        logger = ConfigUtils.getLogger(Spooler.class.getCanonicalName());
        filesToSend = addFilesToSend();
        FileWatcher watcher = new FileWatcher((new File(sourceDirPath))).addListener(new FileAdapter() {
            @Override
            public void onCreated(FileEvent event) {
                File file = event.getFile();
                if (file.getName().endsWith(".done")) {
                    FileElement element;
                    try {
                        element = readMetadata(file);
                        long xxhash = getXXHash64(element.getFile());
                        element.setXXHash(xxhash);
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
    }

    private BlockingQueue<FileElement> addFilesToSend() {
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
                xxhash = getXXHash64(element.getFile());
                element.setXXHash(xxhash);
                logger.log(Level.INFO, "xxHash64 checksum for the file " + element.getFile().getName()
                        + " is " + String.format("%016x", element.getXXHash()));
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            return filesToSend;
        }
    }

    private FileElement readMetadata(File file) throws IOException {
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

    private void writeMetadata(FileElement element) throws IOException {
        String fileName = catalogDirPath + "/" + element.getFile().getName().replaceAll(".root", ".done");
        FileWriter writeFile = new FileWriter(fileName);

        writeFile.write("surl" + ":" + element.getSurl() + "\n");
        writeFile.write("size" + ":" + element.getSize() + "\n");
        writeFile.write("ctime" + ":" + element.getCtime() + "\n");
        writeFile.write("run" + ":" + element.getRun() + "\n");
        writeFile.write("meta" + ":" + element.getMetaaccPeriod() + "\n");
        writeFile.write("md5" + ":" + element.getMd5() + "\n");
        writeFile.write("guid" + ":" + element.getGuid() + "\n");
        writeFile.close();
    }

    private void checkTransferStatus(int exitCode, FileElement element, String xxhash) throws IOException {
        long delayTime;
        int badTransferDelayTime = 10;
        String fileXXHash = String.format("%016x", element.getXXHash());
        String md5Checksum;

        if (exitCode == successfulTransfer && fileXXHash.equals(xxhash)) {
            Main.nrFilesSent.getAndIncrement();
            logger.log(Level.INFO, "The " + element.getFile().getName() + " file is successfully sent!");
            logger.log(Level.INFO, "Total number of files successfully transferred: " + Main.nrFilesSent.get());
            if (md5Option && (element.getMd5() == null)) {
                md5Checksum = IOUtils.getMD5(element.getFile());
                element.setMd5(md5Checksum);
                md5Checksums.add(element);
                logger.log(Level.INFO, "MD5 checksum for the file " + element.getFile().getName()
                        + " is " + element.getMd5());
            }
            writeMetadata(element);
            deleteSource(element);
        } else {
            Main.nrFilesFailed.getAndIncrement();
            logger.log(Level.WARNING, "Transmission of the " + element.getFile().getName() + " file failed!");
            logger.log(Level.INFO, "Total number of files whose transmission failed: " + Main.nrFilesFailed.get());
            element.setNrTries(element.getNrTries() + 1);
            delayTime = Math.min(Math.max((1 << element.getNrTries()), badTransferDelayTime), maxBackoff);
            logger.log(Level.INFO, "The delay time of the file is: " + delayTime);
            element.setTime(System.currentTimeMillis() + delayTime * 1000);
            logger.log(Level.INFO, "The transmission time of the file is: " + element.getTime());
            filesToSend.add(element);
        }
    }

    private static void deleteSource(FileElement element) {
        new File(element.getFile().getAbsolutePath()).delete();
        new File(element.getMetaFilePath()).delete();
    }

    public void run() {
        FileElement file;
        EosCommand command;
        String xxhash;

        try {
            while (true) {
                logger.log(Level.INFO, "Total number of files to be transferred: " + filesToSend.size());

                file = filesToSend.take();
                if (file == null)
                    continue;

                logger.log(Level.INFO, "Total number of files transmitted in parallel: "
                        + Main.nrFilesOnSend.getAndIncrement());

                command = Eos.transfer(file);
                xxhash = command.getOutput().toString().split(" ")[3].replace("\n", "");
                logger.log(Level.INFO, "Received xxhash checksum: " + xxhash + " for "
                        + file.getFile().getName());

                if (!command.isStatus())
                    checkTransferStatus(badTransfer, file, xxhash);
                else
                    checkTransferStatus(successfulTransfer, file, xxhash);

                Main.nrFilesOnSend.getAndDecrement();
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private long getXXHash64(File file) {
        long checksum = 0;
        XXHashFactory factory;
        BufferedInputStream buffStream;
        InputStream input;

        try {
            input = new FileInputStream(file);
            buffStream = new BufferedInputStream(input);
            factory = XXHashFactory.fastestInstance();
            StreamingXXHash64 hash64 = factory.newStreamingHash64(0);
            byte[] buffer = new byte[8192];
            for (;;) {
                int read = buffStream.read(buffer);
                if (read == -1) {
                    break;
                }
                hash64.update(buffer, 0, read);
            }
            checksum = hash64.getValue();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return checksum;
    }

}
