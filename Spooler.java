import alien.config.ConfigUtils;
import alien.io.IOUtils;
import net.jpountz.xxhash.StreamingXXHash64;
import net.jpountz.xxhash.XXHashFactory;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Spooler {
    private BlockingQueue<FileElement> filesToSend;
    private List<FileElement> md5Checksums;
    private List<FileElement> xxHashChecksums;
    private final String sourceDirPath;
    private final Boolean md5Option;
    private final int maxBackoff;
    private final Logger logger;

    // Constants
    private final int badTransfer = 1;
    private final int successfulTransfer = 0;

    public Spooler(String sourceDirPath, int maxBackoff, Boolean md5Option) {
        this.sourceDirPath = sourceDirPath;
        this.maxBackoff = maxBackoff;
        this.md5Option = md5Option;
        md5Checksums = new ArrayList<>();
        xxHashChecksums = new ArrayList<>();
        logger = ConfigUtils.getLogger(Spooler.class.getCanonicalName());
        filesToSend = addFilesToSend();
        FileWatcher watcher = new FileWatcher((new File(sourceDirPath))).addListener(new FileAdapter() {
            @Override
            public void onCreated(FileEvent event) {
                FileElement file = new FileElement(event.getFile());
                filesToSend.add(file);
                xxHashChecksum(file);
                //System.out.println(filesToSend);
            }
        });
        watcher.watch();
    }

    private BlockingQueue<FileElement> addFilesToSend() {
        try {
            int i;
            File dir = new File(sourceDirPath);
            File[] files = dir.listFiles();

            filesToSend = new DelayQueue<>();

            for (i = 0; i < files.length; i++) {
                filesToSend.add(new FileElement(files[i]));
            }

            for (FileElement file : filesToSend)
                xxHashChecksum(file);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            return filesToSend;
        }
    }

    public BlockingQueue<FileElement> getFilesToSend() {
        return filesToSend;
    }

    private void checkTransferStatus(int exitCode, FileElement element, String xxhash) {
        long delayTime;
	int badTransferDelayTime = 10;

        if (exitCode == successfulTransfer && element.getXxhash().equals(xxhash)) {
            Main.nrFilesSent.getAndIncrement();
            logger.log(Level.INFO, "The " + element.getFile().getName() + " file is successfully sent!");
            logger.log(Level.INFO, "Total number of files successfully transferred: " + Main.nrFilesSent.get());
            if (md5Option)
                md5Checksum(element);
            Eos.delete(element);
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

    public void run() {
        FileElement file;
        EosCommand command;
        String xxhash;

        try {
            while (true) {
                logger.log(Level.INFO, "Total number of files to be transferred: " + filesToSend.size());

                file = filesToSend.take();

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

    private void md5Checksum(FileElement element) {
        String checksum;

        try {
            checksum = IOUtils.getMD5(element.getFile());
            element.setMd5(checksum);
            md5Checksums.add(element);
            logger.log(Level.INFO, "MD5 checksum for the file " + element.getFile().getName()
                    + " is " + element.getMd5());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void xxHashChecksum(FileElement element) {
        long checksum;
        XXHashFactory factory;
        File file = element.getFile();
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
            element.setXxhash(String.format("%016x", checksum));
            xxHashChecksums.add(element);
            logger.log(Level.INFO, "xxHash64 checksum for the file " + element.getFile().getName()
                    + " is " + element.getXxhash());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
