package spooler;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.config.ConfigUtils;
import alien.io.IOUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.Timing;
import lia.util.process.ExternalProcess.ExitStatus;

/**
 * @author asuiu
 * @since March 30, 2021
 */
public class Spooler implements Runnable {
	private final Logger logger = ConfigUtils.getLogger(Spooler.class.getCanonicalName());
	private final Monitor monitor = MonitorFactory.getMonitor(Spooler.class.getCanonicalName());

	// Constants
	private final int badTransfer = 1;
	private final int successfulTransfer = 0;

	private final FileElement toTransfer;

	Spooler(FileElement element) {
		this.toTransfer = element;
	}

	private static void writeCMetadata(FileElement element) throws IOException {
		String destPath = Main.spoolerProperties.gets("catalogDir", Main.defaultCatalogDir)
				+ element.getMetaFilePath().substring(element.getMetaFilePath().lastIndexOf('/'));
		String srcPath = element.getMetaFilePath();

		Files.move(Paths.get(srcPath), Paths.get(destPath), StandardCopyOption.REPLACE_EXISTING);
	}

	private boolean checkDataIntegrity(FileElement element, String xxhash) throws IOException {
		long metaXXHash;
		String fileXXHash;

		if (element.getXXHash() == 0) {
			try (Timing t = new Timing(monitor, "xxhash_execution_time");
					FileWriter writeFile = new FileWriter(element.getMetaFilePath(), true)) {

				metaXXHash = IOUtils.getXXHash64(element.getFile());
				element.setXXHash(metaXXHash);
				writeFile.write("xxHash64" + ": " + element.getXXHash() + "\n");

				monitor.addMeasurement("xxhash_file_size", element.getSize());
				monitor.incrementCounter("nr_xxhash_ops");
			}
		}

		fileXXHash = String.format("%016x", Long.valueOf(element.getXXHash()));
		logger.log(Level.INFO, "xxHash64 checksum for the file "
				+ element.getFile().getName() + " is " + fileXXHash);

		return fileXXHash.equals(xxhash);
	}

	private void computeMD5(FileElement element) throws IOException {
		try (FileWriter writeFile = new FileWriter(element.getMetaFilePath(), true)) {
			String md5Checksum;

			md5Checksum = IOUtils.getMD5(element.getFile());
			element.setMd5(md5Checksum);
			writeFile.write("md5" + ": " + element.getMd5() + "\n");

			logger.log(Level.INFO, "MD5 checksum for the file " + element.getFile().getName()
					+ " is " + element.getMd5());
		}
	}

	private void checkTransferStatus(int exitCode, FileElement element, String xxhash)
			throws IOException, InterruptedException {
		long delayTime;

		if (exitCode == successfulTransfer && checkDataIntegrity(element, xxhash)) {
			Main.nrFilesSent.getAndIncrement();
			logger.log(Level.INFO, "The " + element.getFile().getName() + " file is successfully sent!");
			logger.log(Level.INFO, "Total number of files successfully transferred: " + Main.nrFilesSent.get());
			monitor.incrementCacheHits("transferred_files");
			monitor.addMeasurement("nr_transmitted_bytes", element.getSize());

			if (Main.spoolerProperties.getb("md5Enable", Main.defaultMd5Enable)
					&& (element.getMd5() == null)) {
				monitor.incrementCounter("nr_md5_ops");
				monitor.addMeasurement("md5_file_size", element.getSize());
				try (Timing t = new Timing(monitor, "md5_execution_time")) {
					computeMD5(element);
				}
			}

			Eos.transfer(element.getMetaFilePath(), element.getSurl().concat(".meta"),
					element.getSeioDaemons(), new File(element.getMetaFilePath()).length(), 0);

			// TODO log actual transfer status for .meta too
			logger.log(Level.INFO, "The " + element.getMetaFilePath() + " file is successfully sent!");

			writeCMetadata(element);
			deleteSource(element);
		}
		else {
			Main.nrFilesFailed.getAndIncrement();
			logger.log(Level.WARNING, "Transmission of the " + element.getFile().getName() + " file failed!");
			logger.log(Level.INFO, "Total number of files whose transmission failed: " + Main.nrFilesFailed.get());
			monitor.incrementCacheMisses("transferred_files");

			element.setNrTries(element.getNrTries() + 1);
			delayTime = Math.min(1 << element.getNrTries(),
					Main.spoolerProperties.geti("maxBackoff", Main.defaultMaxBackoff));

			logger.log(Level.INFO, "The delay time of the file is: " + delayTime);

			element.setTime(System.currentTimeMillis() + delayTime * 1000);

			logger.log(Level.INFO, "The transmission time of the file is: " + element.getTime());

			Main.transferWatcher.addElement(element);
		}
	}

	private void deleteSource(FileElement element) {
		if (!element.getFile().delete()) {
			logger.log(Level.WARNING, "Could not delete source file " + element.getFile().getAbsolutePath());
		}
	}

	private void transferFile(FileElement element) {
		ExitStatus command;
		String xxhash = null;

		try {
			command = Eos.transfer(element.getFile().getAbsolutePath(), element.getSurl(), element.getSeioDaemons(),
					element.getFile().length(), element.getNrTries());
			if (command.getStdOut().contains("checksum=xxhash64")) {
				xxhash = command.getStdOut().split("checksum=xxhash64")[1].trim();
				logger.log(Level.INFO, "Received xxhash checksum: " + xxhash + " for "
						+ element.getFile().getName());
			}
			else
				logger.log(Level.WARNING, "Could not receive the xxhash from the transfer command");

			if (command.getExtProcExitStatus() != 0)
				checkTransferStatus(badTransfer, element, xxhash);
			else
				checkTransferStatus(successfulTransfer, element, xxhash);
		}
		catch (InterruptedException | IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		logger.log(Level.INFO, "Total number of files transmitted in parallel: " + Main.nrFilesOnSend.getAndIncrement());

		try {
			transferFile(toTransfer);
		}
		finally {
			Main.nrFilesOnSend.getAndDecrement();
		}
	}
}
