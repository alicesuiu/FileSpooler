package spooler;

import alien.catalogue.GUIDUtils;
import alien.config.ConfigUtils;
import lazyj.ExtProperties;
import java.io.*;
import java.nio.file.*;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author asuiu
 * @since March 30, 2021
 */
public class FileWatcher implements Runnable {
    private final Logger logger = ConfigUtils.getLogger(FileWatcher.class.getCanonicalName());
    private AtomicInteger nrFilesWatched = new AtomicInteger(0);
    private final File directory;
    private Map<String, FileExecutor> executors;
    private final String command;

    FileWatcher(File directory, Map<String, FileExecutor> executors, String command) {
        this.directory = directory;
        this.executors = executors;
        this.command = command;
    }

    @Override
    public void run() {
        addFilesToSend(directory.getAbsolutePath());

        try (WatchService watchService = FileSystems.getDefault().newWatchService()) {
            Path path = Paths.get(directory.getAbsolutePath());
            path.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);

            WatchKey key;
            while ((key = watchService.take()) != null) {
                for (WatchEvent<?> event : key.pollEvents()) {
                    Path filePath = Paths.get(directory.getAbsolutePath() + "/" + event.context());
                    File file = filePath.toFile();

                    if (file.getName().endsWith(".done")) {
                        FileElement element = readMetadata(file);
                        if (element == null)
                            continue;
                        addElement(element);
                        logger.log(Level.INFO, Thread.currentThread().getName()
                                + " processed a number of " + nrFilesWatched.incrementAndGet() + " files");
                        logger.log(Level.INFO, "The file " + element.getFile().getAbsolutePath() + " from "
                                + directory.getAbsolutePath() + " was queued");
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
            thread.setName("Watcher Thread for " + directory.getAbsolutePath() + " directory");
        }
    }

    private void addFilesToSend(String sourceDirPath) {
        try {
            int i;
            File dir = new File(sourceDirPath);
            File[] files = dir.listFiles();

            assert files != null;
            for (i = 0; i < files.length; i++) {
                if (files[i].getName().endsWith(".done")) {
                    FileElement element = readMetadata(files[i]);
                    if (element == null)
                        continue;
                    addElement(element);
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private void addElement(FileElement element) {
        int nrThreads;

        if (command.equals("transfer")) {
            if (!executors.containsKey(element.getPriority())) {
                nrThreads = Main.spoolerProperties.geti("queue." + element.getPriority() + ".threads",
                        Main.spoolerProperties.geti("queue.default.threads", Main.defaultTransferThreads));
                FileExecutor executor = new FileExecutor(new DelayQueue<>(), nrThreads);
                executors.put(element.getPriority(), executor);
                executor.getThreadPool().submit(new Spooler(executor.getQueue()));
            }
            executors.get(element.getPriority()).getQueue().add(element);
        } else {
            if (executors.isEmpty()) {
                nrThreads = Main.spoolerProperties.geti("queue.reg.threads", Main.defaultRegistrationThreads);
                FileExecutor executor = new FileExecutor(new LinkedBlockingDeque<>(), nrThreads);
                executors.put("low", executor);
                executor.getThreadPool().submit(new Registrator(executor.getQueue()));
            }
            executors.get("low").getQueue().add(element);
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
        String surl, run, LHCPeriod, md5, uuid, lurl, curl, type, seName, seioDaemons, path, priority;
        long size, ctime, xxhash;
        UUID guid;

        try(InputStream inputStream = new FileInputStream(file);
            FileWriter writeFile = new FileWriter(file.getAbsolutePath(), true)) {

            ExtProperties prop = new ExtProperties(inputStream);

            lurl = prop.gets("lurl", null);
            run = prop.gets("run", null);
            LHCPeriod = prop.gets("LHCPeriod", null);

            if (lurl == null || run == null || LHCPeriod == null) {
                logger.log(Level.WARNING, "Missing mandatory attributes");
                path = Main.spoolerProperties.gets("errorDir", Main.defaultErrorDir)
                        + file.getAbsolutePath().substring(file.getAbsolutePath().lastIndexOf('/'));
                Files.move(Paths.get(file.getAbsolutePath()), Paths.get(path), StandardCopyOption.REPLACE_EXISTING);
                return null;
            }

            type = prop.gets("type", null);
            size = prop.getl("size", 0);
            ctime = prop.getl("ctime", 0);
            uuid = prop.gets("guid", null);

            surl = prop.gets("surl", null);
            curl = prop.gets("curl", null);
            md5 = prop.gets("md5", null);
            xxhash = prop.getl("xxHash64", 0);

            seName = prop.gets("seName", null);
            seioDaemons = prop.gets("seioDaemons", null);
            priority = prop.gets("priority", null);

            if (type == null || type.length() == 0) {
                type = "raw";
                writeFile.write("type" + ": " + type + "\n");
            }

            if (size == 0) {
                size = Files.size(Paths.get(lurl));
                writeFile.write("size" + ": " + size + "\n");
            }

            if (ctime == 0) {
                ctime = new File(lurl).lastModified();
                writeFile.write("ctime" + ": " + ctime + "\n");
            }

            if (uuid == null || uuid.length() == 0) {
                guid = GUIDUtils.generateTimeUUID();
                writeFile.write("guid" + ": " + guid + "\n");
            } else
                guid = UUID.fromString(uuid);

            if (surl == null || surl.length() == 0) {
                surl = generateURL("/eos/test", LHCPeriod, run,
                        type, lurl.substring(lurl.lastIndexOf('/')));
                writeFile.write("surl" + ": " + surl + "\n");
            }

            if (curl == null || curl.length() == 0) {
                curl = generateURL("/localhost/localdomain/user/j/jalien", LHCPeriod, run,
                        type, lurl.substring(lurl.lastIndexOf('/')));
                writeFile.write("curl" + ": " + curl + "\n");
            }

            if (seName == null || seName.length() == 0) {
                seName = Main.spoolerProperties.gets("seName", Main.defaultSEName);
                writeFile.write("seName" + ": " + seName + "\n");
            }

            if (seioDaemons == null || seioDaemons.length() == 0) {
                seioDaemons = Main.spoolerProperties.gets("seioDaemons", Main.defaultseioDaemons);
                writeFile.write("seioDaemons" + ": " + seioDaemons + "\n");
            }

            if (priority == null || priority.length() == 0) {
                priority = "low";
                writeFile.write("priority" + ": " + priority + "\n");
            }
        }

        return new FileElement(md5, surl, size, run, guid, ctime, LHCPeriod,
                file.getAbsolutePath(), xxhash, lurl, type, curl, seName, seioDaemons, priority);
    }

}
