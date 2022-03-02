package spooler;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.config.ConfigUtils;
import alien.io.IOUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;

/**
 * @author asuiu
 * @since March 30, 2021
 */
class FileElement implements Delayed {
	private final File file;
	private int nrTries;
	private long time;
	private String md5;
	private long xxhash;
	private String surl;
	private final String curl;
	private final long size;
	private final String run;
	private final UUID guid;
	private final long ctime;
	private final String LHCPeriod;
	private final String metaFilePath;
	private final String type;
	private final String seName;
	private final String seioDaemons;
	private final String priority;
	private final boolean isMetadata;
	private final String TFOrbits;
	private final int persistent;

	private static final Logger logger = ConfigUtils.getLogger(FileElement.class.getCanonicalName());
	private static final Monitor monitor = MonitorFactory.getMonitor(FileElement.class.getCanonicalName());

	FileElement(final String md5, final String surl, final long size, final String run,
			final UUID guid, final long ctime, final String LHCPeriod, final String metaFilePath,
			final long xxhash, final String lurl, final String type, final String curl, final String seName,
			final String seioDaemons, final String priority, final boolean isMetadata, final String TFOrbits,
				final int persistent) {
		this.md5 = md5;
		this.surl = surl;
		this.curl = curl;
		this.size = size;
		this.run = run;
		this.guid = guid;
		this.ctime = ctime;
		this.LHCPeriod = LHCPeriod;
		this.metaFilePath = metaFilePath;
		this.xxhash = xxhash;
		this.type = type;
		this.seName = seName;
		this.seioDaemons = seioDaemons;
		this.priority = priority;
		this.isMetadata = isMetadata;
		this.TFOrbits = TFOrbits;
		this.persistent = persistent;
		nrTries = 0;
		time = System.currentTimeMillis();
		file = new File(lurl);
	}

	File getFile() {
		return file;
	}

	int getNrTries() {
		return nrTries;
	}

	long getTime() {
		return time;
	}

	String getMd5() {
		return md5;
	}

	long getXXHash() {
		return xxhash;
	}

	String getMetaFilePath() {
		return metaFilePath;
	}

	String getSurl() {
		return surl;
	}

	long getSize() {
		return size;
	}

	String getRun() {
		return run;
	}

	UUID getGuid() {
		return guid;
	}

	long getCtime() {
		return ctime;
	}

	String getLHCPeriod() {
		return LHCPeriod;
	}

	String getCurl() {
		return curl;
	}

	String getSeName() {
		return seName;
	}

	String getSeioDaemons() {
		return seioDaemons;
	}

	String getType() {
		return type;
	}

	String getPriority() {
		return priority;
	}

	public String getTFOrbits() {
		return TFOrbits;
	}

	public int getPersistent() {
		return persistent;
	}

	boolean isMetadata() {
		return isMetadata;
	}

	void setXXHash(final long xxhash) {
		this.xxhash = xxhash;
	}

	void setMd5(final String md5) {
		this.md5 = md5;
	}

	@Override
	public String toString() {
		final String sb = "FileElement{" + "file=" + file +
				", nrTries=" + nrTries +
				", time=" + time +
				", md5='" + md5 + '\'' +
				", xxhash=" + xxhash +
				", surl='" + surl + '\'' +
				", curl='" + curl + '\'' +
				", size=" + size +
				", run='" + run + '\'' +
				", guid=" + guid +
				", ctime=" + ctime +
				", LHCPeriod='" + LHCPeriod + '\'' +
				", metaFilePath='" + metaFilePath + '\'' +
				", type='" + type + '\'' +
				", seName='" + seName + '\'' +
				", seioDaemons='" + seioDaemons + '\'' +
				", priority='" + priority + '\'' +
				", isMetadata=" + isMetadata +
				", TFOrbits='" + TFOrbits + '\'' +
				'}';
		return sb;
	}

	@Override
	public long getDelay(final TimeUnit timeUnit) {
		long difference = time - System.currentTimeMillis();
		difference /= 1000;
		return timeUnit.convert(difference, TimeUnit.SECONDS);
	}

	@Override
	public int compareTo(final Delayed delayed) {
		return Long.compare(getDelay(TimeUnit.SECONDS), delayed.getDelay(TimeUnit.SECONDS));
	}

	void computeDelay() {
		long delayTime;

		final int removeChars = String.valueOf(nrTries).length();
		nrTries += 1;
		delayTime = Math.min(1 << nrTries,
				Main.spoolerProperties.geti("maxBackoff", Main.defaultMaxBackoff));

		String filename = surl.substring(0, surl.lastIndexOf('.'));
		if (nrTries > 1) {
			filename = filename.substring(0, filename.length() - removeChars);
		}
		else {
			filename += "_";
		}
		filename += nrTries;
		final String extension = surl.substring(surl.lastIndexOf(".") + 1);
		surl = filename + "." + extension;

		logger.log(Level.INFO, "The delay time of the file is: " + delayTime);
		time = System.currentTimeMillis() + delayTime * 1000;
		logger.log(Level.INFO, "The transmission time of the file is: " + time);
	}

	void computeMD5() {
		try {
			String md5Checksum;
			md5Checksum = IOUtils.getMD5(file);
			md5 = md5Checksum;
			logger.log(Level.INFO, "MD5 checksum for the file " + surl
					+ " is " + md5);

			if (!isMetadata) {
				try (FileWriter writeFile = new FileWriter(metaFilePath, true)) {
					writeFile.write("md5" + ": " + md5 + "\n");
				}
			}
		}
		catch (final IOException e) {
			logger.log(Level.WARNING, "Could not compute md5 for "
					+ file.getAbsolutePath(), e.getMessage());
		}
	}

	String getMetaSurl() {
		final StringBuilder metaSurl = new StringBuilder(surl.concat(".meta"));
		final int index = surl.lastIndexOf(type);
		metaSurl.replace(index, type.length() + index, type + "_metadata");

		return metaSurl.toString();
	}

	String getMetaCurl() {
		return curl.concat(".meta").replace(type, type + "_metadata");
	}

	boolean existFile() {
		final String src = file.getAbsolutePath();

		if (!Files.exists(Paths.get(src))) {
			logger.log(Level.WARNING, "File " + src + " is no longer found on disk and will not be attempted furher.");
			if (!isMetadata) {
				final String path = Main.spoolerProperties.gets("errorDir", Main.defaultErrorDir)
						+ metaFilePath.substring(metaFilePath.lastIndexOf('/'));
				Main.moveFile(logger, metaFilePath, path.replace("done", "missing"));
				monitor.incrementCounter("error_files");
			}
			return false;
		}
		return true;
	}
}
