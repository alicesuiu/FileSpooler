package metacreator;

import alien.config.ConfigUtils;
import alien.io.xrootd.XrootdFile;
import lazyj.ExtProperties;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {
    static ExtProperties metacreatorProperties;
    static final String defaultSEName = "ALICE::CERN::EOSALICEO2";
    static final String defaultseioDaemons = "root://eosaliceo2.cern.ch:1094";
    private static final String defaultProcessFilesPath = "/home/jalien/metadata_tool/process_files";
    static final String defaultMetadataDir = "/home/jalien/metadata_tool/metaDir";
    static final String defaultRegistrationDir = "/data/epn2eos_tool/daqSpool";
    private static final int defaultThreads = 4;
    private static BlockingQueue<XrootdFile> processFiles = new LinkedBlockingQueue<>();
    private static BlockingQueue<String> listDirs = new LinkedBlockingQueue<>();
    private static Logger logger = ConfigUtils.getLogger(Main.class.getCanonicalName());
    static AtomicInteger nrFilesCreated = new AtomicInteger(0);

    public static void main(String[] args) {
        File processFile;
        metacreatorProperties = ConfigUtils.getConfiguration("metacreator");

        if (metacreatorProperties == null) {
            logger.log(Level.WARNING, "Cannot find metacreator config file");
            return;
        }

        logger.log(Level.INFO,"Storage Element Name: " + metacreatorProperties.gets("seName", defaultSEName));
        logger.log(Level.INFO,"Storage Element seioDaemons: " + metacreatorProperties.gets("seioDaemons", defaultseioDaemons));
        logger.log(Level.INFO,"Process files path: " + metacreatorProperties.gets("processFiles", defaultProcessFilesPath));
        logger.log(Level.INFO,"Metadata Dir Path: " + metacreatorProperties.gets("metaDir", defaultMetadataDir));
        logger.log(Level.INFO, "Registration Dir Path: " + metacreatorProperties.gets("registrationDir", defaultRegistrationDir));

        processFile = new File(metacreatorProperties.gets("processFiles", defaultProcessFilesPath));
        if (!processFile.exists() || processFile.length() == 0) {
            logger.log(Level.WARNING,"The file that contains the list of files to be processed does not exist or is empty");
            return;
        }

        if (!sanityCheckDir(Paths.get(metacreatorProperties.gets("metaDir", defaultMetadataDir))))
            return;

        if (!sanityCheckDir(Paths.get(metacreatorProperties.gets("registrationDir", defaultRegistrationDir))))
            return;

        try(BufferedReader reader = new BufferedReader(new FileReader(metacreatorProperties.gets("processFiles", defaultProcessFilesPath)))) {
            String path;
            while ((path = reader.readLine()) != null) {
                listDirs.add(path.trim());
            }
        } catch (IOException e) {
            logger.log(Level.WARNING,"Caught exception while trying to read from file "
                    + metacreatorProperties.gets("processFiles", defaultProcessFilesPath), e);
        }

        Thread[] listingThreads = new Thread[metacreatorProperties.geti("queue.list.threads", defaultThreads)];
        for (int i = 0; i < metacreatorProperties.geti("queue.list.threads", defaultThreads); i++) {
            listingThreads[i] = new Thread(new ListingThread(listDirs, processFiles));
            listingThreads[i].start();
        }

        Thread[] processThreads = new Thread[metacreatorProperties.geti("queue.process.threads", defaultThreads)];
        for (int i = 0; i < metacreatorProperties.geti("queue.process.threads", defaultThreads); i++) {
            processThreads[i] = new Thread(new MetaCreator(processFiles));
            processThreads[i].start();
        }

    }

    private static boolean sanityCheckDir(final Path path) {
        File directory;

        if (Files.isSymbolicLink(path)) {
            try {
                directory = Files.readSymbolicLink(path).toFile();
            }
            catch (final IOException e) {
                logger.log(Level.WARNING,"Caught exception! " + path.toAbsolutePath() + " is a symbolic link that cannot be followed", e);
                return false;
            }
        }
        else
            directory = path.toFile();

        if (!directory.exists()) {
            if (!directory.mkdirs()) {
                logger.log(Level.WARNING,"Could not create directory " + directory.getAbsolutePath());
                return false;
            }
        }

        if (!directory.isDirectory()) {
            logger.log(Level.WARNING,directory.getAbsolutePath() + " is not a directory");
            return false;
        }

        if (!directory.canWrite() && !directory.canRead() && !directory.canExecute()) {
            logger.log(Level.WARNING,"Could not read, write and execute on directory "
                    + directory.getAbsolutePath());
            return false;
        }

        try {
            final Path tmpFile = Files.createTempFile(Paths.get(directory.getAbsolutePath()), null, null);
            Files.delete(tmpFile);
        }
        catch (final IOException e) {
            logger.log(Level.WARNING,"Could not create/delete file inside the "
                    + directory.getAbsolutePath() + " directory", e);
            return false;
        }

        return true;
    }

    static void moveFile(final Logger log, final String src, final String dest) {
        try {
            Files.move(Paths.get(src), Paths.get(dest), StandardCopyOption.REPLACE_EXISTING);
        }
        catch (final IOException e) {
            log.log(Level.WARNING, "Could not move metadata file: " + src, e.getMessage());
        }
    }
}
