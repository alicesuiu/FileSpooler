package spooler;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.logging.LogRecord;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;

/**
 * @author asuiu
 * @since September 30, 2021
 */
public class RotateHandler extends StreamHandler {
	private static String prefixPath, logfilePath;

	private final ZoneId z;
	private ZonedDateTime currentDate, tomorrowStart;
	private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("YYYY-MM-dd");

	private FileOutputStream fileOutputStream;

	/**
	 * @throws FileNotFoundException
	 */
	public RotateHandler() throws FileNotFoundException {
		z = ZoneId.of("Europe/Zurich");
		currentDate = ZonedDateTime.now(z);
		tomorrowStart = currentDate.toLocalDate().plusDays(1).atStartOfDay(z);

		try {
			prefixPath = "/home/jalien/epn2eos_logs/" + InetAddress.getLocalHost().getHostName();
		}
		catch (@SuppressWarnings("unused") final UnknownHostException e) {
			prefixPath = Main.defaultLogsDir;
		}

		if (!Main.sanityCheckDir(Paths.get(prefixPath)))
			return;

		logfilePath = prefixPath + "/alien-all-" + currentDate.format(formatter) + ".log";

		fileOutputStream = new FileOutputStream(logfilePath, true);
		setOutputStream(fileOutputStream);
		setFormatter(new SimpleFormatter());
	}

	@Override
	public synchronized void publish(final LogRecord record) {
		if (!isLoggable(record))
			return;

		final Duration duration = Duration.between(Instant.now(), tomorrowStart);
		if (duration.isNegative() || !Files.exists(Paths.get(logfilePath)) || fileOutputStream == null) {
			try {
				close();
				currentDate = ZonedDateTime.now(z);
				logfilePath = prefixPath + "/alien-all-" + currentDate.format(formatter) + ".log";
				fileOutputStream = new FileOutputStream(logfilePath, true);
				setOutputStream(fileOutputStream);
			}
			catch (@SuppressWarnings("unused") final IOException e) {
				setOutputStream(System.err);
				fileOutputStream = null;
			}
		}

		super.publish(record);
		flush();
	}
}
