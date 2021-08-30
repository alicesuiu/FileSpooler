package spooler;

import alien.catalogue.GUIDUtils;
import alien.config.ConfigUtils;
import lazyj.ExtProperties;
import java.io.*;
import java.nio.file.*;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author asuiu
 * @since March 30, 2021
 */
class FileWatcher implements Runnable {
    private Logger logger = ConfigUtils.getLogger(Main.class.getCanonicalName());
    private AtomicInteger nrFilesWatched = new AtomicInteger(0);
    private final File directory;
    private BlockingQueue<FileElement> filesToSend;

    FileWatcher(File directory, BlockingQueue<FileElement> filesToSend) {
        this.directory = directory;
        this.filesToSend = filesToSend;
    }

    @Override
    public void run() {
        addFilesToSend(Main.spoolerProperties.gets("metadataDir", Main.defaultMetadataDir));

        try (WatchService watchService = FileSystems.getDefault().newWatchService()) {
            Path path = Paths.get(Main.spoolerProperties.gets("metadataDir", Main.defaultMetadataDir));
            path.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);

            WatchKey key;
            while ((key = watchService.take()) != null) {
                for (WatchEvent<?> event : key.pollEvents()) {
                    Path filePath = Paths.get(Main.spoolerProperties.gets("metadataDir", Main.defaultMetadataDir) + "/" + event.context());
                    File file = filePath.toFile();

                    if (file.getName().endsWith(".done")) {
                        FileElement element = readMetadata(file);
                        filesToSend.add(element);
                        logger.log(Level.INFO, Thread.currentThread().getName()
                                + " processed a number of " + nrFilesWatched.incrementAndGet() + " files");
                        logger.log(Level.INFO, "The file " + element.getFile().getAbsolutePath() + " was queued");
                    }
                }
                key.reset();
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    void watch() {
        if (directory.exists()) {
            Thread thread = new Thread(this);
            thread.setDaemon(true);
            thread.start();
            thread.setName("Watcher Thread");
        }
    }

    private void addFilesToSend(String sourceDirPath) {
        try {
            int i;
            File dir = new File(sourceDirPath);
            File[] files = dir.listFiles();

            assert files != null;
            for (i = 0; i < files.length; i++) {
                if (files[i].getName().endsWith(".done"))
                    filesToSend.add(readMetadata(files[i]));
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private String generateURL(String prefix, String period,
                                      String run, String type, String filename) {

        String url = "";

        url += prefix + "/";
        url += (new Date().getYear() + 1900) + "/";
        url += period + "/";
        url += run  + "/";
        url += type;
        url += filename;

        return url;
    }

    private FileElement readMetadata(File file) throws IOException {
        String surl, run, LHCPeriod, md5, uuid, lurl, curl, type, seName;
        long size, ctime, xxhash;
        UUID guid;

        try(InputStream inputStream = new FileInputStream(file); FileWriter writeFile = new FileWriter(file.getAbsolutePath(), true)) {
            ExtProperties prop = new ExtProperties(inputStream);

            lurl = prop.gets("lurl");
            run = prop.gets("run");
            LHCPeriod = prop.gets("LHCPeriod");

            type = prop.gets("type", null);
            size = prop.getl("size", 0);
            ctime = prop.getl("ctime", 0);
            uuid = prop.gets("guid", null);

            surl = prop.gets("surl", null);
            curl = prop.gets("curl", null);
            md5 = prop.gets("md5", null);
            xxhash = prop.getl("xxHash64", 0);

            seName = prop.gets("seName", null);

            if (type == null) {
                type = "raw";
                writeFile.write("type" + ": " + type + "\n");
            }

            if (size == 0) {
                size = Files.size(Paths.get(lurl));
                writeFile.write("size" + ": " + size + "\n");
            }

            if (ctime == 0) {
                ctime = file.lastModified();
                writeFile.write("ctime" + ": " + ctime + "\n");
            }

            if (uuid == null) {
                guid = GUIDUtils.generateTimeUUID();
                writeFile.write("guid" + ": " + guid + "\n");
            } else
                guid = UUID.fromString(uuid);

            if (surl == null) {
                surl = generateURL("/eos/test", LHCPeriod, run,
                        type, lurl.substring(lurl.lastIndexOf('/')));
                writeFile.write("surl" + ": " + surl + "\n");
            }

            if (curl == null) {
                curl = generateURL("/localhost/localdomain/user/j/jalien", LHCPeriod, run,
                        type, lurl.substring(lurl.lastIndexOf('/')));
                writeFile.write("curl" + ": " + curl + "\n");
            }

            if (seName == null) {
                seName = Main.eosSE.getName();
                writeFile.write("seName" + ": " + seName + "\n");
            }
        }

        return new FileElement(md5, surl, size, run, guid, ctime, LHCPeriod,
                file.getAbsolutePath(), xxhash, lurl, type, curl, seName);
    }

}
