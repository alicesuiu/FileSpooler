import alien.config.ConfigUtils;
import alien.io.IOUtils;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Spooler implements Runnable {
    private BlockingQueue<FileElement> filesToSend;
    private List<FileElement> md5Checksums;
    private final Logger logger;

    // Constants
    private final int badTransfer = 1;
    private final int successfulTransfer = 0;

    public Spooler(BlockingQueue<FileElement> filesToSend) {
        md5Checksums = new ArrayList<>();
        logger = ConfigUtils.getLogger(Spooler.class.getCanonicalName());
        this.filesToSend = filesToSend;
    }

    private void writeMetadata(FileElement element) throws IOException {
        String fileName = Main.spoolerProperties.gets("catalogDir", Main.defaultCatalogDir)
            + "/" + element.getFile().getName().replaceAll(".root", ".done");
        FileWriter writeFile = new FileWriter(fileName);

        writeFile.write("surl" + ":" + element.getDurl() + "\n");
        writeFile.write("size" + ":" + element.getSize() + "\n");
        writeFile.write("ctime" + ":" + element.getCtime() + "\n");
        writeFile.write("run" + ":" + element.getRun() + "\n");
        writeFile.write("meta" + ":" + element.getMetaaccPeriod() + "\n");
        writeFile.write("md5" + ":" + element.getMd5() + "\n");
        writeFile.write("guid" + ":" + element.getGuid() + "\n");
        writeFile.write("xxHash64" + ":" + element.getXXHash() + "\n");
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
            if (Main.spoolerProperties.getb("md5Enable", Main.defaultMd5Enable) && (element.getMd5() == null)) {
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
            delayTime = Math.min(Math.max((1 << element.getNrTries()), badTransferDelayTime),
                Main.spoolerProperties.geti("maxBackoff", Main.defaultMaxBackoff));
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
}
