package spooler;

import java.io.*;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.catalogue.GUIDUtils;
import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import lazyj.ExtProperties;

/**
 * @author asuiu
 * @since March 30, 2021
 */
class FileWatcher implements Runnable {
	private static final Logger logger = ConfigUtils.getLogger(FileWatcher.class.getCanonicalName());
    private static final Monitor monitor = MonitorFactory.getMonitor(FileWatcher.class.getCanonicalName());
	private AtomicInteger nrFilesWatched = new AtomicInteger(0);
	private final File directory;
	Map<String, ScheduledThreadPoolExecutor> executors = new ConcurrentHashMap<>();
	private final boolean isTransfer;

	FileWatcher(File directory, boolean isTransfer) {
		this.directory = directory;
		this.isTransfer = isTransfer;
	}

	@Override
	public void run() {
		addFilesToSend(directory.getAbsolutePath());

		try (FileSystem fs = FileSystems.getDefault(); WatchService watchService = fs.newWatchService()) {
			Path path = Paths.get(directory.getAbsolutePath());
			path.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);

			WatchKey key;
			while (Main.shouldRun && (key = watchService.take()) != null) {
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
						logger.log(Level.INFO, "The file " + file.getAbsolutePath() + " was queued");

						monitor.incrementCounter("nr_files_processed_by_"
								+ (isTransfer ? "transfer_watcher" : "reg_watcher"));
					}
				}

				key.reset();
			}
		}
		catch (IOException | InterruptedException e) {
			logger.log(Level.WARNING,
					"Could not create " + (isTransfer ? "transfer_watcher" : "reg_watcher"), e.getMessage());
			System.exit(-1);
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

	void shutdown() {
		for (Map.Entry<String, ScheduledThreadPoolExecutor> entry : executors.entrySet()) {
			ScheduledThreadPoolExecutor executor = entry.getValue();
			executor.shutdown();
			try {
				executor.awaitTermination(30, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				logger.log(Level.WARNING,
						"Caught interrupted exception while trying to shutdown an executor", e.getMessage());
			}
		}
	}

	private void addFilesToSend(String sourceDirPath) {
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
	}

	void addElement(FileElement element) {
		ScheduledExecutorService executor = executors.computeIfAbsent(isTransfer ? element.getPriority() : "low", (k) -> {
			int nrThreads;

			if (isTransfer)
				nrThreads = Main.spoolerProperties.geti("queue." + element.getPriority() + ".threads",
						Main.spoolerProperties.geti("queue.default.threads", Main.defaultTransferThreads));
			else
				nrThreads = Main.spoolerProperties.geti("queue.reg.threads", Main.defaultRegistrationThreads);

			ScheduledThreadPoolExecutor service = new ScheduledThreadPoolExecutor(nrThreads, (r) -> new Thread(r, k + "/" + isTransfer));
			service.setKeepAliveTime(1L, TimeUnit.MINUTES);
			service.allowCoreThreadTimeOut(true);

			return service;
		});

		executor.schedule(isTransfer ? new Spooler(element) : new Registrator(element), element.getDelay(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
	}

	private static String generateURL(String prefix, String period,
			String run, String type, String filename) {

		String url = "";

		url += prefix + "/";
		url += (new Date().getYear() + 1900) + "/";
		url += period + "/";
		url += run + "/";
		url += type;

		url += filename;

		return url;
	}

	private FileElement readMetadata(File file) {
		String surl, run, LHCPeriod, md5, uuid, lurl, curl, type, seName, seioDaemons, path, priority;
		long size, ctime, xxhash;
		UUID guid;

		try (InputStream inputStream = new FileInputStream(file);
				FileWriter writeFile = new FileWriter(file.getAbsolutePath(), true)) {

			ExtProperties prop = new ExtProperties(inputStream);

			lurl = prop.gets("lurl", null);
			run = prop.gets("run", null);
			LHCPeriod = prop.gets("LHCPeriod", null);

			if (lurl == null || run == null || LHCPeriod == null
                    || lurl.isBlank() || run.isBlank() || LHCPeriod.isBlank()) {
				logger.log(Level.WARNING, "Missing mandatory attributes in file: " + file.getAbsolutePath());
				path = Main.spoolerProperties.gets("errorDir", Main.defaultErrorDir) + "/" + file.getName();
				Main.moveFile(logger, file.getAbsolutePath(), path);
				return null;
			}

			if (isTransfer && !Files.exists(Paths.get(lurl))) {
				logger.log(Level.WARNING, "File " + lurl + " is no longer in "
						+ Paths.get(lurl).getParent().toAbsolutePath()
						+ " and will not be attempted furher.");
				path = Main.spoolerProperties.gets("errorDir", Main.defaultErrorDir) + "/" + file.getName();
				Main.moveFile(logger, file.getAbsolutePath(), path);
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

			if (type == null || type.isBlank()) {
				type = "raw";
				writeFile.write("type" + ": " + type + "\n");
			}

			if (size == 0) {
				size = Files.size(Paths.get(lurl));
				writeFile.write("size" + ": " + size + "\n");
			} else if (isTransfer) {
                long realSize = Files.size(Paths.get(lurl));
                if (size != realSize) {
                    logger.log(Level.WARNING, "Size of " + lurl
                            + " is different than what the metadata indicates (" + size + " vs " + realSize + ")");
					path = Main.spoolerProperties.gets("errorDir", Main.defaultErrorDir) + "/" + file.getName();
                    Main.moveFile(logger, file.getAbsolutePath(), path);
                    return null;
                }
			}

			if (ctime == 0) {
				ctime = new File(lurl).lastModified();
				writeFile.write("ctime" + ": " + ctime + "\n");
			}

			if (uuid == null || uuid.isBlank() || !GUIDUtils.isValidGUID(uuid)) {
				guid = GUIDUtils.generateTimeUUID();
				writeFile.write("guid" + ": " + guid + "\n");
			}
			else
				guid = UUID.fromString(uuid);

			if (surl == null || surl.isBlank()) {
				surl = generateURL("/" + type, LHCPeriod, run,
						type, lurl.substring(lurl.lastIndexOf('/')));
				writeFile.write("surl" + ": " + surl + "\n");
			}

			if (curl == null || curl.isBlank()) {
				curl = generateURL("/alice/data", LHCPeriod, run,
						type, lurl.substring(lurl.lastIndexOf('/')));
				writeFile.write("curl" + ": " + curl + "\n");
			}

			if (seName == null || seName.isBlank()) {
				seName = Main.spoolerProperties.gets("seName", Main.defaultSEName);
				writeFile.write("seName" + ": " + seName + "\n");
			}

			if (seioDaemons == null || seioDaemons.isBlank()) {
				seioDaemons = Main.spoolerProperties.gets("seioDaemons", Main.defaultseioDaemons);
				writeFile.write("seioDaemons" + ": " + seioDaemons + "\n");
			}

			if (priority == null || priority.isBlank()) {
				priority = "low";
				writeFile.write("priority" + ": " + priority + "\n");
			}
		} catch (IOException e) {
			logger.log(Level.WARNING, "Could not read/write the metadata file " + file.getAbsolutePath(), e.getMessage());
			path = Main.spoolerProperties.gets("errorDir", Main.defaultErrorDir) + "/" + file.getName();
			Main.moveFile(logger, file.getAbsolutePath(), path);
			return null;
		}

		return new FileElement(md5, surl, size, run, guid, ctime, LHCPeriod,
				file.getAbsolutePath(), xxhash, lurl, type, curl, seName, seioDaemons, priority, false);
	}
}
