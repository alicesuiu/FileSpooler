package spooler;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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
						+ element.getFile().getAbsolutePath(), e);
			}
		}

		fileXXHash = String.format("%016x", Long.valueOf(element.getXXHash()));
		logger.log(Level.INFO, "xxHash64 checksum for the file "
				+ element.getFile().getName() + " is " + fileXXHash);

		return fileXXHash.equals(xxhash);
	}

	private static void computeMD5(FileElement element) {
		try (FileWriter writeFile = new FileWriter(element.getMetaFilePath(), true)) {
			String md5Checksum;

			md5Checksum = IOUtils.getMD5(element.getFile());
			element.setMd5(md5Checksum);
			writeFile.write("md5" + ": " + element.getMd5() + "\n");

			logger.log(Level.INFO, "MD5 checksum for the file " + element.getFile().getName()
					+ " is " + element.getMd5());
		}
		catch (IOException e) {
			logger.log(Level.WARNING, "Could not compute md5 for "
					+ element.getFile().getAbsolutePath(), e);
		}
	}

	private static void onSuccess(FileElement element, boolean isMetadata) {
        logger.log(Level.INFO, "Successfully transfered: " + element.getFile().getName());
        monitor.addMeasurement("nr_transmitted_bytes", element.getSize());

        if (isMetadata) {
            Main.nrMetaFilesSent.getAndIncrement();
            logger.log(Level.INFO, "Total number of metadata files successfully transferred: "
                    + Main.nrMetaFilesSent.get());
            monitor.incrementCacheHits("metadata_transferred_files");
        } else {
            Main.nrDataFilesSent.getAndIncrement();
            logger.log(Level.INFO, "Total number of data files successfully transferred: "
                    + Main.nrDataFilesSent.get());
            monitor.incrementCacheHits("data_transferred_files");

            if (Main.spoolerProperties.getb("md5Enable", Main.defaultMd5Enable)
                    && (element.getMd5() == null)) {
                monitor.addMeasurement("md5_file_size", element.getSize());
                try (Timing t = new Timing(monitor, "md5_execution_time")) {
                    computeMD5(element);
                }
            }

            if (!element.getFile().delete()) {
                logger.log(Level.WARNING, "Could not delete source file "
                        + element.getFile().getAbsolutePath());
            }
        }
	}

	private static void onFail(FileElement element, boolean isMetadata) {
        logger.log(Level.WARNING, "Transmission of the " + element.getFile().getName() + " file failed!");

	    if (isMetadata) {
            Main.nrMetaFilesFailed.getAndIncrement();
            logger.log(Level.INFO, "Total number of metadata files whose transmission failed: "
                    + Main.nrMetaFilesFailed.get());
            monitor.incrementCacheMisses("metadata_transferred_files");
        }  else {
            Main.nrDataFilesFailed.getAndIncrement();
            logger.log(Level.INFO, "Total number of data files whose transmission failed: "
                    + Main.nrDataFilesFailed.get());
            monitor.incrementCacheMisses("data_transferred_files");

            element.computeDelay();
            Main.transferWatcher.addElement(element);
        }
	}

	private static boolean transfer(FileElement element, boolean isMetadata) {
		try {
			SE se = new SE(element.getSeName(), 1, "", "", element.getSeioDaemons());
			GUID guid = new GUID(element.getGuid());
			guid.size = element.getSize();
			PFN pfn = new PFN(element.getSeioDaemons() + "/" + element.getSurl(), guid, se);

			new Xrootd().put(pfn, element.getFile(), false);

			onSuccess(element, isMetadata);
			return true;
		}
		catch (IOException e) {
			logger.log(Level.WARNING, "Transfer failed with exception", e);
			onFail(element, isMetadata);
		}

		return false;
	}

	private void transferFile() {
		boolean status;

		status = transfer(toTransfer, false);
		if (status) {
			FileElement metadataFile = new FileElement(
					null,
					toTransfer.getSurl().concat(".meta"),
					new File(toTransfer.getMetaFilePath()).length(),
					toTransfer.getRun(),
					GUIDUtils.generateTimeUUID(),
					new File(toTransfer.getMetaFilePath()).lastModified(),
					toTransfer.getLHCPeriod(),
					null,
					0,
					toTransfer.getMetaFilePath(),
					toTransfer.getType(),
					toTransfer.getCurl().concat(".meta"),
					toTransfer.getSeName(),
					toTransfer.getSeioDaemons(),
					null);

			transfer(metadataFile, true);

            String destPath = Main.spoolerProperties.gets("registrationDir", Main.defaultRegistrationDir)
                    + toTransfer.getMetaFilePath().substring(toTransfer.getMetaFilePath().lastIndexOf('/'));
            String srcPath = toTransfer.getMetaFilePath();

            Main.moveFile(logger, srcPath, destPath);
		}
	}

	@Override
	public void run() {
		logger.log(Level.INFO, "Total number of files transmitted in parallel: "
				+ Main.nrFilesOnSend.incrementAndGet());
		try {
			transferFile();
		}
		finally {
			Main.nrFilesOnSend.decrementAndGet();
		}
	}
}
