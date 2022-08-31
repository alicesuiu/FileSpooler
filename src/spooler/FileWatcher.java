package spooler;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
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
	private final AtomicInteger nrFilesWatched = new AtomicInteger(0);
	private final File directory;
	Map<String, ScheduledThreadPoolExecutor> executors = new ConcurrentHashMap<>();
	private final boolean isTransfer;
	BlockingQueue<File> processNewFiles = new LinkedBlockingDeque<>();
	private Thread intermediateThread = null;
	private Thread myself = null;

	FileWatcher(final File directory, final boolean isTransfer) {
		this.directory = directory;
		this.isTransfer = isTransfer;
	}

	@Override
	public void run() {
		addFilesToSend(directory.getAbsolutePath());

		try (FileSystem fs = FileSystems.getDefault(); WatchService watchService = fs.newWatchService()) {
			final Path path = Paths.get(directory.getAbsolutePath());
			path.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);

			WatchKey key;
			while (Main.shouldRun && (key = watchService.take()) != null) {
				try {
					for (final WatchEvent<?> event : key.pollEvents()) {
						final Path filePath = Paths.get(directory.getAbsolutePath() + "/" + event.context());
						final File file = filePath.toFile();

						if (file.getName().endsWith(".done")) {
							processNewFiles.add(file);
						}
					}
				}
				catch (final Exception e) {
					logger.log(Level.WARNING, "Exception handling one event", e);
				}
				finally {
					key.reset();
				}
			}
		}
		catch (IOException | InterruptedException e) {
			logger.log(Level.WARNING,
					"Could not create " + (isTransfer ? "transfer_watcher" : "reg_watcher"), e.getMessage());
		}
	}

	void watch() {
		if (directory.exists()) {
			myself = new Thread(this);
			myself.setDaemon(true);
			myself.start();
			myself.setName("Watcher Thread I for " + directory.getAbsolutePath() + " directory");

			intermediateThread = new Thread(() -> {
				while (Main.shouldRun) {
					try {
						final File file = processNewFiles.take();
						final FileElement element = readMetadata(file);
						if (element == null)
							continue;
						addElement(element);

						logger.log(Level.INFO, Thread.currentThread().getName()
								+ " processed a number of " + nrFilesWatched.incrementAndGet() + " files");
						logger.log(Level.INFO, "The file " + file.getAbsolutePath() + " was queued");

						monitor.incrementCounter("nr_files_processed_by_"
								+ (isTransfer ? "transfer_watcher" : "reg_watcher"));
					}
					catch (final InterruptedException e) {
						logger.log(Level.WARNING, "Intermediate watcher thread was interrupted while waiting", e);
					}
				}
			});
			intermediateThread.setName("Watcher Thread II for " + directory.getAbsolutePath() + " directory");
			intermediateThread.setDaemon(true);
			intermediateThread.start();
		}
	}

	void shutdown() {
		for (final Map.Entry<String, ScheduledThreadPoolExecutor> entry : executors.entrySet()) {
			final ScheduledThreadPoolExecutor executor = entry.getValue();
			executor.shutdown();
			try {
				executor.awaitTermination(30, TimeUnit.SECONDS);
			}
			catch (final InterruptedException e) {
				logger.log(Level.WARNING,
						"Caught interrupted exception while trying to shutdown an executor", e.getMessage());
			}
		}

		if (intermediateThread != null)
			intermediateThread.interrupt();

		if (myself != null)
			myself.interrupt();
	}

	private void addFilesToSend(final String sourceDirPath) {
		int i;
		final File dir = new File(sourceDirPath);
		final File[] files = dir.listFiles();

		assert files != null;
		for (i = 0; i < files.length; i++) {
			if (files[i].getName().endsWith(".done")) {
				final FileElement element = readMetadata(files[i]);
				if (element == null)
					continue;
				addElement(element);
			}
		}
	}

	void addElement(final FileElement element) {
		final ScheduledExecutorService executor = executors.computeIfAbsent(isTransfer ? element.getPriority() : "low", (k) -> {
			int nrThreads;

			if (isTransfer)
				nrThreads = Main.spoolerProperties.geti("queue." + element.getPriority() + ".threads",
						Main.spoolerProperties.geti("queue.default.threads", Main.defaultTransferThreads));
			else
				nrThreads = Main.spoolerProperties.geti("queue.reg.threads", Main.defaultRegistrationThreads);

			final FileScheduleExecutor service = new FileScheduleExecutor(nrThreads, (r) -> new Thread(r, k + "/" + isTransfer));
			service.setKeepAliveTime(1L, TimeUnit.MINUTES);
			service.allowCoreThreadTimeOut(true);

			return service;
		});

		executor.schedule(isTransfer ? new Spooler(element) : new Registrator(element), element.getDelay(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
	}

	private static String generateURL(final String prefix, final String period,
			final String run, final String type, final String filename, long ctime) {

		String url = "";

		url += prefix + "/";
		url += (new Date().getYear() + 1900) + "/";
		url += period + "/";
		url += run + "/";
		url += type + "/";

		ctime = (ctime / (1000 * 60 * 10)) * (1000 * 60 * 10);
		Timestamp ts = new Timestamp(ctime);
		ZoneId z = ZoneId.of("Europe/Zurich");
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HHmm");
		String dateTime = ts.toInstant().atZone(z).format(formatter);

		url += dateTime;
		url += filename;

		return url;
	}

	private FileElement readMetadata(final File file) {
		String surl, run, LHCPeriod, md5, uuid, lurl, curl, type, seName,
				seioDaemons, path, priority, TFOrbits, det_composition;
		long size, ctime, xxhash;
		UUID guid;
		int persistent;

		try (InputStream inputStream = new FileInputStream(file);
				FileWriter writeFile = new FileWriter(file.getAbsolutePath(), true)) {

			final ExtProperties prop = new ExtProperties(inputStream);

			lurl = prop.gets("lurl", null);
			run = prop.gets("run", null);
			LHCPeriod = prop.gets("LHCPeriod", null);

			if (lurl == null || run == null || LHCPeriod == null
					|| lurl.isBlank() || run.isBlank() || LHCPeriod.isBlank()) {
				logger.log(Level.WARNING, "Missing mandatory attributes in file: " + file.getAbsolutePath());
				path = Main.spoolerProperties.gets("errorDir", Main.defaultErrorDir) + "/" + file.getName();
				Main.moveFile(logger, file.getAbsolutePath(), path.replace("done", "invalid"));
				monitor.incrementCounter("error_files");
				return null;
			}

			if (isTransfer && !Files.exists(Paths.get(lurl))) {
				logger.log(Level.WARNING, "File " + lurl + " is no longer found on disk and will not be attempted furher.");
				path = Main.spoolerProperties.gets("errorDir", Main.defaultErrorDir) + "/" + file.getName();
				Main.moveFile(logger, file.getAbsolutePath(), path.replace("done", "missing"));
				monitor.incrementCounter("error_files");
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
			TFOrbits = prop.gets("TFOrbits", null);
			persistent = prop.geti("persistent", 0);
			det_composition = prop.gets("det_composition", null);

			if (type == null || type.isBlank()) {
				type = "other";
				writeFile.write("type" + ": " + type + "\n");
			}

			type = type.toLowerCase();

			if (type.equals("calibration")) {
				type = "calib";
			}

			if (!type.equals("raw") && !type.equals("calib") && !type.equals("other")) {
				logger.log(Level.WARNING, "Unsupported type: " + type + " for file: " + file.getAbsolutePath() +
						"Type can only be raw, calib or other");
				path = Main.spoolerProperties.gets("errorDir", Main.defaultErrorDir) + "/" + file.getName();
				Main.moveFile(logger, file.getAbsolutePath(), path.replace("done", "invalid"));
				monitor.incrementCounter("error_files");
				return null;
			}

			if (size == 0) {
				size = Files.size(Paths.get(lurl));
				writeFile.write("size" + ": " + size + "\n");
			}
			else
				if (isTransfer) {
					final long realSize = Files.size(Paths.get(lurl));
					if (size != realSize) {
						logger.log(Level.WARNING, "Size of " + lurl
								+ " is different than what the metadata indicates (" + size + " vs " + realSize + ")");
						path = Main.spoolerProperties.gets("errorDir", Main.defaultErrorDir) + "/" + file.getName();
						Main.moveFile(logger, file.getAbsolutePath(), path);
						monitor.incrementCounter("error_files");
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

			/* the detector list contains a single detector
			 * example:
			 * det_composition: TPC
			 * LHCPeriod: OCT
			 */
			if (det_composition != null && !det_composition.contains(",") && !LHCPeriod.contains("_")) {
				LHCPeriod = LHCPeriod + "_" + det_composition;
				LHCPeriod = LHCPeriod.replaceAll("[\\n\\t ]", "");
			}

			if (surl == null || surl.isBlank()) {
				surl = generateURL("/" + type, LHCPeriod, run,
						type, lurl.substring(lurl.lastIndexOf('/')), ctime);
				writeFile.write("surl" + ": " + surl + "\n");
			}

			if (curl == null || curl.isBlank()) {
				curl = generateURL("/alice/data", LHCPeriod, run,
						type, lurl.substring(lurl.lastIndexOf('/')), ctime);
				writeFile.write("curl" + ": " + curl + "\n");
			}

			if (priority == null || priority.isBlank()) {
				priority = "low";
				writeFile.write("priority" + ": " + priority + "\n");
			}
		}
		catch (final IOException e) {
			logger.log(Level.WARNING, "Could not read/write the metadata file " + file.getAbsolutePath(), e.getMessage());
			path = Main.spoolerProperties.gets("errorDir", Main.defaultErrorDir) + "/" + file.getName();
			Main.moveFile(logger, file.getAbsolutePath(), path);
			monitor.incrementCounter("error_files");
			return null;
		}

		logger.log(Level.INFO, "Metadata attributes for: " + lurl + " "
				+ "surl: " + surl + ", size: " + size + ", curl: " + curl
				+ ", priority: " + priority + ", type: " + type + ", guid: " + guid
				+ ", LHCPeriod: " + LHCPeriod + ", run: " + run + ", ctime: " + ctime
				+ ", metadataFile: " + file.getAbsolutePath());

		return new FileElement(md5, surl, size, run, guid, ctime, LHCPeriod,
				file.getAbsolutePath(), xxhash, lurl, type, curl, seName, seioDaemons,
				priority, false, TFOrbits, persistent);
	}
}
