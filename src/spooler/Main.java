package spooler;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import lazyj.ExtProperties;

/**
 * @author asuiu
 * @since March 30, 2021
 */
public class Main {
	static AtomicInteger nrFilesOnSend = new AtomicInteger(0);
	static AtomicInteger nrFilesOnRegister = new AtomicInteger(0);
	static AtomicInteger nrFilesSent = new AtomicInteger(0);
	static AtomicInteger nrFilesRegistered = new AtomicInteger(0);
	static AtomicInteger nrFilesFailed = new AtomicInteger(0);
	static AtomicInteger nrFilesRegFailed = new AtomicInteger(0);
	static ExtProperties spoolerProperties;

	private static Logger logger = ConfigUtils.getLogger(Main.class.getCanonicalName());

	/**
	 * Activity monitoring
	 */
	private static final Monitor monitor = MonitorFactory.getMonitor(Main.class.getCanonicalName());

	// Default Constants
	private static final String defaultMetadataDir = System.getProperty("user.home") + "/epn2eos";
	static final String defaultCatalogDir = System.getProperty("user.home") + "/daqSpool";
	static final String defaultErrorDir = System.getProperty("user.home") + "/error";
	static final String defaultSEName = "ALICE::CERN::EOSALICEO2";
	static final String defaultseioDaemons = "root://eosaliceo2.cern.ch:1094/";
	static final boolean defaultMd5Enable = false;
	static final int defaultMaxBackoff = 10;
	static final int defaultTransferThreads = 4;
	static final int defaultRegistrationThreads = 1;

	static FileWatcher transferWatcher;
	static FileWatcher registrationWatcher;

	/**
	 * Entry point
	 * 
	 * @param args
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws InterruptedException {
		spoolerProperties = ConfigUtils.getConfiguration("spooler");

		logger.log(Level.INFO, "Metadata Dir Path: " + spoolerProperties.gets("metadataDir", defaultMetadataDir));
		logger.log(Level.INFO, "Exponential Backoff Limit: " + spoolerProperties.geti("maxBackoff", defaultMaxBackoff));
		logger.log(Level.INFO, "MD5 option: " + spoolerProperties.getb("md5Enable", defaultMd5Enable));
		logger.log(Level.INFO, "Catalog Dir Path: " + spoolerProperties.gets("catalogDir", defaultCatalogDir));
		logger.log(Level.INFO, "Error Dir Path: " + spoolerProperties.gets("errorDir", defaultErrorDir));
		logger.log(Level.INFO, "Maximum Number of Transfer Threads: " + spoolerProperties.geti("queue.default.threads", defaultTransferThreads));
		logger.log(Level.INFO, "Maximum Number of Registration Threads: " + spoolerProperties.geti("queue.reg.threads", defaultRegistrationThreads));
		logger.log(Level.INFO, "EOS Server Path: " + spoolerProperties.gets("seName", defaultSEName));
		logger.log(Level.INFO, "EOS seioDaemons: " + spoolerProperties.gets("seioDaemons", defaultseioDaemons));

		transferWatcher = new FileWatcher(new File(spoolerProperties.gets("metadataDir", defaultMetadataDir)), true);
		transferWatcher.watch();

		registrationWatcher = new FileWatcher(new File(spoolerProperties.gets("catalogDir", defaultCatalogDir)), false);
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

			// TODO restul parametrilor
		});

		while (true) {
			Thread.sleep(1000L * 60);
		}
	}
}
