package spooler;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.site.supercomputing.titan.Pair;
import apmon.ApMon;
import apmon.ApMonException;
import lazyj.ExtProperties;
import sun.misc.Signal;

/**
 * @author asuiu
 * @since March 30, 2021
 */
public class Main {
	static AtomicInteger nrFilesOnSend = new AtomicInteger(0);
	static AtomicInteger nrFilesOnRegister = new AtomicInteger(0);
	static AtomicInteger nrDataFilesSent = new AtomicInteger(0);
	static AtomicInteger nrDataFilesReg = new AtomicInteger(0);
	static AtomicInteger nrDataFilesFailed = new AtomicInteger(0);
	static AtomicInteger nrDataFilesRegFailed = new AtomicInteger(0);

	static Map<Long, String> activeRunsPerThread = new ConcurrentHashMap<>();
	static Map<Long, Pair<String, Long>> activeRunsSize = new ConcurrentHashMap<>();

	static ExtProperties spoolerProperties;

	private static Logger logger = ConfigUtils.getLogger(Main.class.getCanonicalName());

	/**
	 * Activity monitoring
	 */
	private static final Monitor monitor = MonitorFactory.getMonitor(Main.class.getCanonicalName());

	/**
	 * Default Constants
	 */
	private static final String defaultMetadataDir = "/data/epn2eos_tool/epn2eos";
	static final String defaultRegistrationDir = "/data/epn2eos_tool/daqSpool";
	static final String defaultErrorDir = "/data/epn2eos_tool/error";
	static final String defaultLogsDir = "/data/epn2eos_tool/logs";
	static final String defaultErrorRegDir = "/data/epn2eos_tool/errorReg";
	static final String defaultSEName = "ALICE::CERN::EOSALICEO2";
	static final String defaultseioDaemons = "root://eosaliceo2.cern.ch:1094";
	static final boolean defaultMd5Enable = false;
	static final int defaultMaxBackoff = 10;
	static final int defaultTransferThreads = 4;
	static final int defaultRegistrationThreads = 1;

	static FileWatcher transferWatcher;
	static FileWatcher registrationWatcher;
	static boolean shouldRun = true;

	private static final String version = "v.1.25";

	/**
	 * Entry point
	 *
	 * @param args
	 * @throws InterruptedException
	 */
	public static void main(final String[] args) throws InterruptedException, ApMonException, IOException {
		spoolerProperties = ConfigUtils.getConfiguration("epn2eos");

		if (!sanityCheckDir(Paths.get(spoolerProperties.gets("metadataDir", defaultMetadataDir))))
			return;
		logger.log(Level.INFO, "Metadata Dir Path: "
				+ spoolerProperties.gets("metadataDir", defaultMetadataDir));

		if (!sanityCheckDir(Paths.get(spoolerProperties.gets("registrationDir", defaultRegistrationDir))))
			return;
		logger.log(Level.INFO, "Registration Dir Path: "
				+ spoolerProperties.gets("registrationDir", defaultRegistrationDir));

		if (!sanityCheckDir(Paths.get(spoolerProperties.gets("errorDir", defaultErrorDir))))
			return;
		logger.log(Level.INFO, "Error Dir Path for transfer: "
				+ spoolerProperties.gets("errorDir", defaultErrorDir));

		if (!sanityCheckDir(Paths.get(spoolerProperties.gets("errorRegDir", defaultErrorRegDir))))
			return;
		logger.log(Level.INFO, "Error Dir Path for registration: "
				+ spoolerProperties.gets("errorRegDir", defaultErrorRegDir));

		logger.log(Level.INFO, "Exponential Backoff Limit: " + spoolerProperties.geti("maxBackoff", defaultMaxBackoff));
		logger.log(Level.INFO, "MD5 option: " + spoolerProperties.getb("md5Enable", defaultMd5Enable));

		logger.log(Level.INFO, "Number of Transfer Threads: " + spoolerProperties.geti("queue.default.threads", defaultTransferThreads));
		logger.log(Level.INFO, "Number of Registration Threads: " + spoolerProperties.geti("queue.reg.threads", defaultRegistrationThreads));

		logger.log(Level.INFO, "Storage Element Name: " + spoolerProperties.gets("seName", defaultSEName));
		logger.log(Level.INFO, "Storage Element seioDaemons: " + spoolerProperties.gets("seioDaemons", defaultseioDaemons));

		ConfigUtils.setApplicationName("epn2eos");

		transferWatcher = new FileWatcher(new File(spoolerProperties.gets("metadataDir", defaultMetadataDir)), true);
		transferWatcher.watch();

		registrationWatcher = new FileWatcher(new File(spoolerProperties.gets("registrationDir", defaultRegistrationDir)), false);
		registrationWatcher.watch();

		monitor.addMonitoring("main", (names, values) -> {
			names.add("active_transfers");
			values.add(Integer.valueOf(nrFilesOnSend.get()));

			names.add("transfer_queues");
			values.add(Integer.valueOf(transferWatcher.executors.size()));

			names.add("transfer_queued_files");
			values.add(Integer.valueOf(transferWatcher.executors.values().stream().mapToInt((s) -> s.getQueue().size()).sum()));

			names.add("transfer_slots");
			values.add(Integer.valueOf(transferWatcher.executors.values().stream().mapToInt((s) -> s.getPoolSize()).sum()));

			names.add("active_registrations");
			values.add(Integer.valueOf(nrFilesOnRegister.get()));

			names.add("registration_queued_files");
			values.add(Integer.valueOf(registrationWatcher.executors.values().stream().mapToInt((s) -> s.getQueue().size()).sum()));

			names.add("registration_slots");
			values.add(Integer.valueOf(registrationWatcher.executors.values().stream().mapToInt((s) -> s.getPoolSize()).sum()));

			names.add("transfer_queued_files_size");
			values.add(Long.valueOf(transferWatcher.executors.values().stream().mapToLong(Main::totalFilesSize).sum()));

			final File directory = new File(spoolerProperties.gets("errorDir", defaultErrorDir));

			if (directory.isDirectory()) {
				final String[] listing = directory.list();

				if (listing != null) {
					int missing = 0;
					int done = 0;
					int invalid = 0;

					for (final String name : listing) {
						if (name.endsWith(".missing"))
							missing++;
						else
							if (name.endsWith(".done"))
								done++;
							else
								if (name.endsWith(".invalid"))
									invalid++;
					}

					names.add("active_missing_error_files");
					values.add(Integer.valueOf(missing));

					names.add("active_invalid_error_files");
					values.add(Integer.valueOf(invalid));

					names.add("active_transfer_error_files");
					values.add(Integer.valueOf(done));
				}
			}

			names.add("active_registration_error_files");
			values.add(Integer.valueOf(totalFilesDir(new File(spoolerProperties.gets("errorRegDir", defaultErrorRegDir)))));

			names.add("active_transfers_epn2eos_files");
			values.add(Integer.valueOf(totalFilesDir(new File(spoolerProperties.gets("metadataDir", defaultMetadataDir)))));

			names.add("version");
			values.add(version);
		});

		final Object lock = new Object();

		Signal.handle(new Signal("TERM"), signal -> {
			shouldRun = false;
			transferWatcher.shutdown();
			registrationWatcher.shutdown();

			logger.log(Level.WARNING, "The epn2eos tool is shutting down");

			synchronized (lock) {
				lock.notifyAll();
			}
		});

		Map<String, Long> prevActiveRuns = new HashMap<>();
		Map<String, Long> prevActiverRunsSize = new HashMap<>();
		while (shouldRun) {
			Map<String, Long> transferActiveRuns = getActiveRunsPerExecutor(transferWatcher.executors, false);
			Map<String, Long>  registerActiveRuns = getActiveRunsPerExecutor(registrationWatcher.executors, false);
			Map<String, Long> currentActiveRuns = new HashMap<>();
			transferActiveRuns.forEach((key, value) -> currentActiveRuns.merge(key, value, Long::sum));
			registerActiveRuns.forEach((key, value) -> currentActiveRuns.merge(key, value, Long::sum));
			activeRunsPerThread.forEach((id, run) -> {
				if (!currentActiveRuns.containsKey(run))
					currentActiveRuns.put(run, 1L);
				else
					currentActiveRuns.put(run, currentActiveRuns.get(run) + 1L);
			});
			sendActiveRunsApMon(prevActiveRuns, currentActiveRuns, false);
			prevActiveRuns = currentActiveRuns;

			Map<String, Long> transferActiveRunsSize = getActiveRunsPerExecutor(transferWatcher.executors, true);
			Map<String, Long> currentActiveRunsSize = new HashMap<>();
			transferActiveRunsSize.forEach((key, value) -> currentActiveRunsSize.merge(key, value, Long::sum));
			activeRunsSize.forEach((id, pair) -> {
				String run = pair.getFirst();
				Long size = pair.getSecond();
				if (!currentActiveRunsSize.containsKey(run))
					currentActiveRunsSize.put(run, size);
				else
					currentActiveRunsSize.put(run, currentActiveRunsSize.get(run) + size);
			});
			sendActiveRunsApMon(prevActiverRunsSize, currentActiveRunsSize, true);
			prevActiverRunsSize = currentActiveRunsSize;

			synchronized (lock) {
				lock.wait(1000L * 60);
			}
		}
	}

	private static Map<String, Long> getActiveRunsPerQueue(final ScheduledThreadPoolExecutor executor, boolean isSize) {
		Map<String, Long> activeRuns = new HashMap<>();

		final BlockingQueue<Runnable> queue = executor.getQueue();
		queue.forEach(future -> {
			if (future instanceof FileScheduleFuture) {
				String run = ((FileScheduleFuture<?>) future).getOperator().getElement().getRun();
				Long cnt = (isSize ? ((FileScheduleFuture<?>) future).getOperator().getElement().getSize() : 1);
				if (!activeRuns.containsKey(run))
					activeRuns.put(run, cnt);
				else
					activeRuns.put(run, activeRuns.get(run) + cnt);
			}
		});
		return activeRuns;
	}

	private static Map<String, Long> getActiveRunsPerExecutor(Map<String, ScheduledThreadPoolExecutor> executors, boolean isSize) {
		Map<String, Long> activeRuns = new HashMap<>();

		executors.forEach((priority, executor) -> {
			Map<String, Long> activeRunsPerQueue =  getActiveRunsPerQueue(executor, isSize);
			activeRunsPerQueue.forEach((key, value) -> activeRuns.merge(key, value, Long::sum));
		});
		return activeRuns;
	}

	private static void sendActiveRunsApMon(Map<String, Long> prevActiveRuns,
											Map<String, Long> currentActiveRuns,
											boolean isSize) throws ApMonException, IOException {
		final Vector<String> paramNames = new Vector<>();
		final Vector<Object> paramValues = new Vector<>();

		currentActiveRuns.forEach((run, cnt) -> {
			paramNames.add(run);
			paramValues.add(cnt);
		});

		prevActiveRuns.keySet().forEach(run -> {
			if (!currentActiveRuns.containsKey(run)) {
				paramNames.add(run);
				paramValues.add(0);
			}
		});

		if (paramNames.size() > 0) {
			StringBuilder result = new StringBuilder();
			for (int i = 0; i < paramNames.size(); i++) {
				result.append("(").append(paramNames.get(i)).append(", ").append(paramValues.get(i)).append(") ");
			}
			logger.log(Level.INFO, "List of active runs: " + result);
			ApMon apmon = MonitorFactory.getApMonSender();
			if (!isSize)
				apmon.sendParameters("epn2eos", "active_runs_cnt", paramNames.size(), paramNames, paramValues);
			else
				apmon.sendParameters("epn2eos", "active_runs_size", paramNames.size(), paramNames, paramValues);
		}
	}
	private static long totalFilesSize(final ScheduledThreadPoolExecutor s) {
		long sum = 0;

		final BlockingQueue<Runnable> queue = s.getQueue();
		for (final Runnable future : queue) {
			if (future instanceof FileScheduleFuture
					&& ((FileScheduleFuture) future).getOperator() instanceof Spooler) {
				Spooler spooler = (Spooler) ((FileScheduleFuture) future).getOperator();
				sum +=  spooler.getElement().getFile().length();
			}
		}

		return sum;
	}

	private static int totalFilesDir(final File directory) {
		if (directory != null && directory.isDirectory()) {
			final String[] listing = directory.list();

			if (listing != null)
				return listing.length;
		}

		return 0;
	}

	static boolean sanityCheckDir(final Path path) {
		File directory;

		if (Files.isSymbolicLink(path)) {
			try {
				directory = Files.readSymbolicLink(path).toFile();
			}
			catch (final IOException e) {
				logger.log(Level.WARNING, "Caught exception! " + path.toAbsolutePath() + " is a symbolic link that cannot be followed", e);
				return false;
			}
		}
		else
			directory = path.toFile();

		if (!directory.exists()) {
			if (!directory.mkdirs()) {
				logger.log(Level.WARNING, "Could not create directory " + directory.getAbsolutePath());
				return false;
			}
		}

		if (!directory.isDirectory()) {
			logger.log(Level.WARNING, directory.getAbsolutePath() + " is not a directory");
			return false;
		}

		if (!directory.canWrite() && !directory.canRead() && !directory.canExecute()) {
			logger.log(Level.WARNING, "Could not read, write and execute on directory "
					+ directory.getAbsolutePath());
			return false;
		}

		try {
			final Path tmpFile = Files.createTempFile(Paths.get(directory.getAbsolutePath()), null, null);
			Files.delete(tmpFile);
		}
		catch (final IOException e) {
			logger.log(Level.WARNING, "Could not create/delete file inside the "
					+ directory.getAbsolutePath() + " directory", e.getMessage());
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
