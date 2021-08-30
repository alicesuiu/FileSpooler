package spooler;

import alien.config.ConfigUtils;
import alien.io.IOUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.Timing;
import lia.util.process.ExternalProcess.ExitStatus;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author asuiu
 * @since March 30, 2021
 */
public class Spooler implements Runnable {
    private BlockingQueue<FileElement> filesToSend;
    private final Logger logger = ConfigUtils.getLogger(Spooler.class.getCanonicalName());
    private final Monitor monitor = MonitorFactory.getMonitor(Spooler.class.getCanonicalName());

    // Constants
    private final int badTransfer = 1;
    private final int successfulTransfer = 0;

    Spooler(BlockingQueue<FileElement> filesToSend) {
        this.filesToSend = filesToSend;
    }

    private void writeCMetadata(FileElement element) throws IOException {
        String destPath = Main.spoolerProperties.gets("catalogDir", Main.defaultCatalogDir)
            + element.getMetaFilePath().substring(element.getMetaFilePath().lastIndexOf('/'));
        String srcPath = element.getMetaFilePath();

        Files.move(Paths.get(srcPath), Paths.get(destPath), StandardCopyOption.REPLACE_EXISTING);
    }

    private boolean checkDataIntegrity(FileElement element, String xxhash) throws IOException {
        long metaXXHash;
        String fileXXHash;

        if (element.getXXHash() == 0) {
            try (Timing t = new Timing(monitor, "xxhash_execution_time");
                 FileWriter writeFile = new FileWriter(element.getMetaFilePath(), true)) {

                metaXXHash = IOUtils.getXXHash64(element.getFile());
                element.setXXHash(metaXXHash);
                writeFile.write("xxHash64" + ": " + element.getXXHash() + "\n");

                monitor.addMeasurement("xxhash_file_size", element.getSize());

                monitor.incrementCounter("nr_xxhash_ops");
            }
        }

        fileXXHash = String.format("%016x", element.getXXHash());
        logger.log(Level.INFO, "xxHash64 checksum for the file "
                + element.getFile().getName() + " is " + fileXXHash);

        return fileXXHash.equals(xxhash);
    }

    private void computeMD5(FileElement element) throws IOException {
        try (FileWriter writeFile = new FileWriter(element.getMetaFilePath(), true)) {
            String md5Checksum;

            md5Checksum = IOUtils.getMD5(element.getFile());
            element.setMd5(md5Checksum);
            writeFile.write("md5" + ": " + element.getMd5() + "\n");

            logger.log(Level.INFO, "MD5 checksum for the file " + element.getFile().getName()
                    + " is " + element.getMd5());
        }
    }

    private void checkTransferStatus(int exitCode, FileElement element, String xxhash) throws IOException {
        long delayTime;

        if (exitCode == successfulTransfer && checkDataIntegrity(element, xxhash)) {
            Main.nrFilesSent.getAndIncrement();
            logger.log(Level.INFO, "The " + element.getFile().getName() + " file is successfully sent!");
            logger.log(Level.INFO, "Total number of files successfully transferred: " + Main.nrFilesSent.get());
            monitor.incrementCounter("files_successfully_transferred");

            if (Main.spoolerProperties.getb("md5Enable", Main.defaultMd5Enable)
                && (element.getMd5() == null)) {
                monitor.incrementCounter("nr_md5_ops");
                monitor.addMeasurement("md5_file_size", element.getSize());
                try (Timing t = new Timing(monitor, "md5_execution_time")) {
                    computeMD5(element);
                }
            }

            writeCMetadata(element);
            deleteSource(element);
        } else {
            Main.nrFilesFailed.getAndIncrement();
            logger.log(Level.WARNING, "Transmission of the " + element.getFile().getName() + " file failed!");
            logger.log(Level.INFO, "Total number of files whose transmission failed: " + Main.nrFilesFailed.get());
            monitor.incrementCounter("files_transmission_failed");

            element.setNrTries(element.getNrTries() + 1);
            delayTime = Math.min(1 << element.getNrTries(),
                Main.spoolerProperties.geti("maxBackoff", Main.defaultMaxBackoff));

            logger.log(Level.INFO, "The delay time of the file is: " + delayTime);

            element.setTime(System.currentTimeMillis() + delayTime * 1000);

            logger.log(Level.INFO, "The transmission time of the file is: " + element.getTime());

            filesToSend.add(element);
        }
    }

    private void deleteSource(FileElement element) {
        if (!new File(element.getFile().getAbsolutePath()).delete()) {
            logger.log(Level.WARNING, "Could not delete source file " + element.getFile().getAbsolutePath());
        }
    }

    @Override
    public void run() {
        FileElement file;
        ExitStatus command;
        String xxhash = null;

        try {
            while (true) {
                logger.log(Level.INFO, "Total number of files to be transferred: " + filesToSend.size());

                file = filesToSend.take();
                if (file == null)
                    continue;

                logger.log(Level.INFO, "Total number of files transmitted in parallel: "
                        + Main.nrFilesOnSend.getAndIncrement());
                monitor.incrementCounter("files_transferred");

                try {
                    monitor.addMeasurement("active_transfers", 1);

                    command = Eos.transfer(file);
                    if (command.getStdOut().contains("checksum=xxhash64")) {
                        xxhash = command.getStdOut().split("checksum=xxhash64")[1].trim();
                        logger.log(Level.INFO, "Received xxhash checksum: " + xxhash + " for "
                                + file.getFile().getName());
                    }
                    else
                        logger.log(Level.WARNING, "Could not receive the xxhash from the transfer command");

                    if (command.getExtProcExitStatus() != 0)
                        checkTransferStatus(badTransfer, file, xxhash);
                    else
                        checkTransferStatus(successfulTransfer, file, xxhash);

                    Main.nrFilesOnSend.getAndDecrement();
                } finally {
                    monitor.addMeasurement("active_transfers", -1);
                }
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
