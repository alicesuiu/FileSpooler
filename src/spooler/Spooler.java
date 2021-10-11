package spooler;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.PFN;
import alien.config.ConfigUtils;
import alien.io.IOUtils;
import alien.io.protocols.Xrootd;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.Timing;
import alien.se.SE;
import lazyj.Format;

/**
 * @author asuiu
 * @since March 30, 2021
 */
class Spooler implements Runnable {
	private static final Logger logger = ConfigUtils.getLogger(Spooler.class.getCanonicalName());
	private static final Monitor monitor = MonitorFactory.getMonitor(Spooler.class.getCanonicalName());
	private final FileElement toTransfer;

	Spooler(FileElement toTransfer) {
		this.toTransfer = toTransfer;
	}

	private static boolean checkDataIntegrity(FileElement element, String xxhash) {
		long metaXXHash;
		String fileXXHash;

		if (xxhash == null)
			return false;

		if (element.getXXHash() == 0) {
			try (Timing t = new Timing(monitor, "xxhash_execution_time");
					FileWriter writeFile = new FileWriter(element.getMetaFilePath(), true)) {

				metaXXHash = IOUtils.getXXHash64(element.getFile());
				element.setXXHash(metaXXHash);
				writeFile.write("xxHash64" + ": " + element.getXXHash() + "\n");

				monitor.addMeasurement("xxhash_file_size", element.getSize());
			}
			catch (IOException e) {
				logger.log(Level.WARNING, "Could not compute xxhash for "
						+ element.getFile().getAbsolutePath(), e.getMessage());
			}
		}

		fileXXHash = String.format("%016x", Long.valueOf(element.getXXHash()));
		logger.log(Level.INFO, "xxHash64 checksum for the file "
				+ element.getFile().getName() + " is " + fileXXHash);

		return fileXXHash.equals(xxhash);
	}

	private static void onSuccess(FileElement element, double transfer_time) {
		DecimalFormat formatter = new DecimalFormat("#.##");
        logger.log(Level.INFO, "Successfully transfered: "
						+ element.getSurl()
						+ " of size " + Format.size(element.getSize())
						+ " with rate " + Format.size(element.getSize() / transfer_time) + "/s"
						+ " in " + formatter.format(transfer_time) + "s");
        monitor.addMeasurement("nr_transmitted_bytes", element.getSize());

		Main.nrDataFilesSent.getAndIncrement();
		logger.log(Level.INFO, "Total number of data files successfully transferred: "
				+ Main.nrDataFilesSent.get());
		monitor.incrementCacheHits("data_transferred_files");

		if (Main.spoolerProperties.getb("md5Enable", Main.defaultMd5Enable)
				&& (element.getMd5() == null)) {
			monitor.addMeasurement("md5_file_size", element.getSize());
			try (Timing t = new Timing(monitor, "md5_execution_time")) {
				element.computeMD5();
			}
		}

		if (!element.getFile().delete()) {
			logger.log(Level.WARNING, "Could not delete source file "
					+ element.getFile().getAbsolutePath());
		}

		String destPath = Main.spoolerProperties.gets("registrationDir", Main.defaultRegistrationDir)
				+ element.getMetaFilePath().substring(element.getMetaFilePath().lastIndexOf('/'));
		String srcPath = element.getMetaFilePath();
		Main.moveFile(logger, srcPath, destPath);
	}

	private static void onFail(FileElement element) {
		Main.nrDataFilesFailed.getAndIncrement();
		logger.log(Level.INFO, "Total number of data files whose transmission failed: "
				+ Main.nrDataFilesFailed.get());
		monitor.incrementCacheMisses("data_transferred_files");

		if (!element.existFile())
			return;

		FileElement metadataFile = new FileElement(
				null,
				element.getMetaSurl(),
				new File(element.getMetaFilePath()).length(),
				element.getRun(),
				GUIDUtils.generateTimeUUID(),
				new File(element.getMetaFilePath()).lastModified(),
				element.getLHCPeriod(),
				null,
				0,
				element.getMetaFilePath(),
				element.getType(),
				element.getMetaCurl(),
				element.getSeName(),
				element.getSeioDaemons(),
				null,
				true,
				null);

		if (!metadataFile.existFile())
			return;

		element.computeDelay();
		Main.transferWatcher.addElement(element);
	}

	private static boolean transfer(FileElement element) {
		try {
			SE se = new SE(element.getSeName(), 1, "", "", element.getSeioDaemons());
			GUID guid = new GUID(element.getGuid());
			guid.size = element.getSize();
			PFN pfn = new PFN(element.getSeioDaemons() + "/" + element.getSurl(), guid, se);
			double transfer_time = 0;

			try (Timing t = new Timing(monitor, "transfer_execution_time")) {
				new Xrootd().put(pfn, element.getFile(), false);
				transfer_time = t.getSeconds();
			}

			onSuccess(element, transfer_time);
			return true;
		}
		catch (IOException e) {
			logger.log(Level.WARNING, "Transfer failed with exception for file: " + element.getSurl()
					+ "\n" + e.getMessage());
			if (e.getMessage().contains("Unable to overwrite existing file - you are write-once user ; " +
					"File exists (destination)")) {
				String path = Main.spoolerProperties.gets("errorDir", Main.defaultErrorDir)
						+ element.getMetaFilePath().substring(element.getMetaFilePath().lastIndexOf('/'));
				Main.moveFile(logger, element.getMetaFilePath(), path);
			} else
				onFail(element);
		}

		return false;
	}

	@Override
	public void run() {
		logger.log(Level.INFO, "Total number of files transmitted in parallel: "
				+ Main.nrFilesOnSend.incrementAndGet());
		try {
			transfer(toTransfer);
		}
		finally {
			Main.nrFilesOnSend.decrementAndGet();
		}
	}
}
