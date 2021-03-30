import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Spooler {
    private BlockingQueue<FileElement> filesToSend;
    private List<FileElement> md5ToRegister;
    private final String eosServerPath;
    private final String eosDirPath;
    private final String sourceDirPath;
    private final Boolean md5Option;
    private final int maxBackoff;
    private final Logger logger;

    // Constants
    private final int badTransfer = 1;
    private final int successfulTransfer = 0;

    public Spooler(String eosServerPath, String eosDirPath, String sourceDirPath, int maxBackoff, Boolean md5Option) {
        this.eosServerPath = eosServerPath;
        this.eosDirPath = eosDirPath;
        this.sourceDirPath = sourceDirPath;
        this.maxBackoff = maxBackoff;
        this.md5Option = md5Option;
        filesToSend = addFilesToSend();
        FileWatcher watcher = new FileWatcher((new File(sourceDirPath))).addListener(new FileAdapter() {
            @Override
            public void onCreated(FileEvent event) {
                filesToSend.add(new FileElement(event.getFile().getName(),
                        event.getFile().length(),
                        event.getFile().getAbsolutePath()));
                System.out.println(filesToSend);
            }
        });
        watcher.watch();
        System.setProperty("java.util.logging.SimpleFormatter.format", "[%1$tF %1$tT] [%4$-2s] %5$s %n");
        logger = Logger.getLogger(Spooler.class.getName());
        md5ToRegister = new ArrayList<>();
    }

    private BlockingQueue<FileElement> addFilesToSend() {
        try {
            int i;
            File dir = new File(sourceDirPath);
            File[] files = dir.listFiles();

            filesToSend = new DelayQueue<>();

            for (i = 0; i < files.length; i++) {
                filesToSend.add(new FileElement(files[i].getName(),
                        files[i].length(),
                        files[i].getAbsolutePath()));
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            return filesToSend;
        }
    }

    public BlockingQueue<FileElement> getFilesToSend() {
        return filesToSend;
    }

    private void send(FileElement file) throws IOException, InterruptedException {
        boolean transfer;
        long timeWaitSend;
        ProcessBuilder shellProcess;
        Process process;
        ProcessWithTimeout ptimeout;
        List<String> cmd = new ArrayList<>();

        cmd.add("eos");
    	cmd.add(eosServerPath);
    	cmd.add("cp");
        cmd.add("file:" + file.getAbsolutePath());
        cmd.add(eosDirPath + "/" + file.getFileName());

        shellProcess = new ProcessBuilder();
        shellProcess.command(cmd);
        process = shellProcess.start();

        ptimeout = new ProcessWithTimeout(process, shellProcess);
        timeWaitSend = getWaitTimeSend(file);
        transfer = ptimeout.waitFor(timeWaitSend, TimeUnit.SECONDS);

        if (!transfer)
            checkTransferStatus(badTransfer, file);
        else
            checkTransferStatus(successfulTransfer, file);
    }

    private long getWaitTimeSend(FileElement file) {
        long bandwith;

        bandwith = (100000 << 3) / Main.nrFilesOnSend.get() / (file.getNrTries() + 1);
        return file.getFileSize() / bandwith;
    }

    private void checkTransferStatus(int exitCode, FileElement file) {
        long delayTime;
	int badTransferDelayTime = 10;

        if (exitCode == successfulTransfer) {
            Main.nrFilesSent.getAndIncrement();
            logger.log(Level.INFO, "The " + file.getFileName() + " file is successfully sent!");
            logger.log(Level.INFO, "Total number of files successfully transferred: " + Main.nrFilesSent.get());
            if (md5Option)
                checksum(file);
            deleteFile(file);
        } else {
            Main.nrFilesFailed.getAndIncrement();
            logger.log(Level.WARNING, "Transmission of the " + file.getFileName() + " file failed!");
            logger.log(Level.INFO, "Total number of files whose transmission failed: " + Main.nrFilesFailed.get());
            file.setNrTries(file.getNrTries() + 1);
            delayTime = Math.min(Math.max((1 << file.getNrTries()), badTransferDelayTime), maxBackoff);
            logger.log(Level.INFO, "The delay time of the file is: " + delayTime);
            file.setTime(System.currentTimeMillis() + delayTime * 1000);
            logger.log(Level.INFO, "The transmission time of the file is: " + file.getTime());
            filesToSend.add(file);
        }
    }

    private void deleteFile(FileElement file) {
        new File(file.getAbsolutePath()).delete();
    }

    public void run() {
        FileElement file;
        try {
            while (true) {
                logger.log(Level.INFO, "Total number of files to be transferred: " + filesToSend.size());

                file = filesToSend.take();

                logger.log(Level.INFO, "Total number of files transmitted in parallel: "
                        + Main.nrFilesOnSend.getAndIncrement());

                send(file);

                Main.nrFilesOnSend.getAndDecrement();
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void checksum(FileElement file) {
        String checksum;
        int md5BufferSize = 8192;

        try {
            FileInputStream fis = new FileInputStream(file.getAbsolutePath());
            MessageDigest md5 = MessageDigest.getInstance("MD5");

            byte[] buffer = new byte[md5BufferSize];
            int numOfBytesRead;
            while ((numOfBytesRead = fis.read(buffer)) > 0) {
                md5.update(buffer, 0, numOfBytesRead);
            }
            byte[] hash = md5.digest();
            checksum = new BigInteger(1, hash).toString(16);

            file.setMd5(checksum);
            md5ToRegister.add(file);
            logger.log(Level.INFO, "MD5 checksum for the file " + file.getFileName() + " is " + file.getMd5());

        } catch (NoSuchAlgorithmException | IOException e) {
            e.printStackTrace();
        }
    }

}
