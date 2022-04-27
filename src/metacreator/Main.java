package metacreator;

import alien.catalogue.GUIDUtils;
import alien.config.ConfigUtils;
import alien.io.xrootd.XrootdFile;
import alien.io.xrootd.XrootdListing;
import lazyj.ExtProperties;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {
    private static ExtProperties metacreatorProperties;
    private static final String defaultSEName = "ALICE::CERN::EOSALICEO2";
    private static final String defaultseioDaemons = "root://eosaliceo2.cern.ch:1094";
    private static final String defaultProcessFilesPath = "/home/jalien/metadata_tool/process_files";
    private static final String defaultMetadataDir = "/home/jalien/metadata_tool/metaDir";
    private static BlockingQueue<XrootdFile> processFiles = new LinkedBlockingQueue<>();
    private static Logger logger = ConfigUtils.getLogger(Main.class.getCanonicalName());

    public static void main(String[] args) {
        metacreatorProperties = ConfigUtils.getConfiguration("metacreator");

        if (metacreatorProperties == null) {
            logger.log(Level.WARNING, "Cannot find metacreator config file");
            return;
        }

        logger.log(Level.INFO,"Storage Element Name: " + metacreatorProperties.gets("seName", defaultSEName));
        logger.log(Level.INFO,"Storage Element seioDaemons: " + metacreatorProperties.gets("seioDaemons", defaultseioDaemons));
        logger.log(Level.INFO,"Process files path: " + metacreatorProperties.gets("processFiles", defaultProcessFilesPath));
        logger.log(Level.INFO,"Metadata Dir Path: " + metacreatorProperties.gets("metaDir", defaultMetadataDir));

        if (!(new File(metacreatorProperties.gets("processFiles", defaultProcessFilesPath))).exists()) {
            logger.log(Level.WARNING,"The file that contains the list of files to be processed does not exist");
            return;
        }

        if (!sanityCheckDir(Paths.get(metacreatorProperties.gets("metaDir", defaultMetadataDir))))
            return;

        String server = metacreatorProperties.gets("seioDaemons", defaultseioDaemons)
                .substring(metacreatorProperties.gets("seioDaemons", defaultseioDaemons).lastIndexOf('/') + 1);

        try(BufferedReader reader = new BufferedReader(new FileReader(metacreatorProperties.gets("processFiles", defaultProcessFilesPath)))) {
            String path;
            while ((path = reader.readLine()) != null) {
                addFiles(server, path.trim());
            }
        } catch (IOException e) {
            logger.log(Level.WARNING,"Caught exception while trying to read from file "
                    + metacreatorProperties.gets("processFiles", defaultProcessFilesPath), e);
        }

        for(XrootdFile file : processFiles) {
            createMetadataFile(file);
        }

    }

    private static void createMetadataFile(XrootdFile file) {
        String seName, seioDaemons, surl, curl, metaFileName;
        UUID guid;
        long size, ctime;

        metaFileName = metacreatorProperties.gets("metaDir", defaultMetadataDir) + "/meta-"
                + file.path.replace("/eos/aliceo2/ls2data/", "").replace('/', '-');
        seName = metacreatorProperties.gets("seName", defaultSEName);
        seioDaemons = metacreatorProperties.gets("seioDaemons", defaultseioDaemons);
        surl = file.path;
        curl = "/alice/data/2021/" + file.path.replace("/eos/aliceo2/ls2data/", "");
        guid = GUIDUtils.generateTimeUUID();
        size = file.size;
        ctime = file.date.getTime();

        try(FileWriter writeFile = new FileWriter(metaFileName, true)) {
            writeFile.write("seName" + ": " + seName + "\n");
            writeFile.write("seioDaemons" + ": " + seioDaemons + "\n");
            writeFile.write("surl" + ": " + surl + "\n");
            writeFile.write("curl" + ": " + curl + "\n");
            writeFile.write("guid" + ": " + guid + "\n");
            writeFile.write("size" + ": " + size + "\n");
            writeFile.write("ctime" + ": " + ctime + "\n");
        } catch (IOException e) {
            logger.log(Level.WARNING,"Caught exception while writing the metadata file for " + metaFileName, e);
        }
    }

    private static void addFiles(String server, String path) throws IOException {
        XrootdListing listing = new XrootdListing(server, path, null);
        Set<XrootdFile> files = listing.getFiles();
        Set<XrootdFile> directories = listing.getDirs();

        if (!files.isEmpty())
            processFiles.addAll(files);

        for (XrootdFile dir : directories) {
            addFiles(server, dir.path);
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
}
