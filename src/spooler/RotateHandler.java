package spooler;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.*;
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
    private DateTimeFormatter formatter =  DateTimeFormatter.ofPattern("YYYY-MM-dd");

    private FileOutputStream fileOutputStream;

    public RotateHandler() throws FileNotFoundException {
        z = ZoneId.of( "Europe/Zurich" );
        currentDate = ZonedDateTime.now(z);
        tomorrowStart = currentDate.toLocalDate().plusDays(1).atStartOfDay(z);

        try {
            prefixPath = "/home/jalien/epn2eos_logs/" + InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
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
    public synchronized void publish(LogRecord record) {
        if (!isLoggable(record))
            return;

        Duration duration = Duration.between(Instant.now(), tomorrowStart);
        if (duration.isNegative() || !Files.exists(Paths.get(logfilePath))) {
            try {

                close();
                currentDate = ZonedDateTime.now(z);
                logfilePath = prefixPath + "/alien-all-" + currentDate.format(formatter) + ".log";
                fileOutputStream = new FileOutputStream(logfilePath, true);
                setOutputStream(fileOutputStream);
            } catch (IOException e) {
                // todo
            }
        }

        super.publish(record);
        flush();
    }
}
