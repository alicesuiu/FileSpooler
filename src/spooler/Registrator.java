package spooler;

import alien.catalogue.Register;
import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.se.SE;
import alien.se.SEUtils;
import alien.user.AliEnPrincipal;
import alien.user.UserFactory;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author asuiu
 * @since August 24, 2021
 */
public class Registrator implements Runnable {
    private final AliEnPrincipal OWNER = UserFactory.getByUsername("jalien");

    private final Logger logger = ConfigUtils.getLogger(Registrator.class.getCanonicalName());
    private final Monitor monitor = MonitorFactory.getMonitor(Registrator.class.getCanonicalName());

    private BlockingQueue<FileElement> filesToRegister;

    Registrator(BlockingQueue<FileElement> filesToRegister) {
        this.filesToRegister = filesToRegister;
    }

    private void registerFile(FileElement element) throws IOException {
        boolean done;

        SE targetSE = SEUtils.getSE(element.getSeName());
        done = Register.register(element.getCurl(),
                targetSE.seioDaemons + "/" + element.getSurl(),
                element.getGuid().toString(), element.getMd5(), element.getSize(),
                element.getSeName(), OWNER);

        if (!done) {
            Main.nrFilesRegFailed.getAndIncrement();
            logger.log(Level.WARNING, "Registering failed for:\nFile : " + element.getCurl()
                    + "\nPFN: " + targetSE.seioDaemons + "/" + element.getSurl()
                    + "\nGUID: " + element.getGuid()
                    + "\nMD5: " + element.getMd5()
                    + "\nSize: " + element.getSize()
                    + "\nSE: " + element.getSeName());
            logger.log(Level.INFO, "Total number of files whose registration failed: " + Main.nrFilesRegFailed.get());
            monitor.incrementCounter("files_registration_failed");
        } else {
            Main.nrFilesRegistered.getAndIncrement();
            logger.log(Level.INFO, "File successfuly registered : " + element.getCurl()
                    + ", " + targetSE.seioDaemons + "/" + element.getSurl()
                    + ", " + element.getGuid()
                    + ", " + element.getMd5()
                    + ", " + element.getSize()
                    + ", " + element.getSeName());
            logger.log(Level.INFO, "Total number of files successfully registered: " + Main.nrFilesRegistered.get());
            monitor.incrementCounter("files_successfully_registered");

            /* if (!new File(element.getMetaFilePath()).delete()) {
                logger.log(Level.WARNING, "Could not delete metadata file " + element.getMetaFilePath());
            } */
        }
    }

    @Override
    public void run() {
        FileElement file;

        try {
            while (true) {
                logger.log(Level.INFO, "Total number of files to be registered: " + filesToRegister.size());

                file = filesToRegister.take();

                monitor.incrementCounter("files_registered");

                registerFile(file);
            }
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }
}
