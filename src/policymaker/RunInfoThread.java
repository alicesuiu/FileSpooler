package policymaker;

import alien.config.ConfigUtils;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RunInfoThread extends Thread {
    private final long DELTA = 300000; // 5 min in ms
    private final long DAY = 86400000; // 24 hours in ms
    private static RunInfoThread instance = null;
    private Object lock = new Object();
    private static Logger logger = ConfigUtils.getLogger(RunInfoThread.class.getCanonicalName());
    private RunInfoThread() {
        super(RunInfoThread.class.getCanonicalName());
        setDaemon(true);
    }
    public static synchronized RunInfoThread getInstance() {
        if (instance == null) {
            instance = new RunInfoThread();
            instance.start();
        }
        return instance;
    }

    @Override
    public void run() {
        long updatedAt = 0;
        while (true) {
            long minTime = ZonedDateTime.now(ZoneId.of("Europe/Zurich"))
                    .minusWeeks(1).toInstant().toEpochMilli() / 1000;
            long currentTime = ZonedDateTime.now(ZoneId.of("Europe/Zurich")).toInstant().toEpochMilli();
            long maxTime = (currentTime - DELTA) / 1000;

            logger.log(Level.INFO, "mintime: " + minTime + ", maxtime: " + maxTime);

            String select = "select rr.run from rawdata_runs rr left outer join rawdata_runs_action ra on"
                + " ra.run=rr.run and action='delete' where mintime >= " + minTime + " and maxtime <= " + maxTime
                + " and daq_transfercomplete IS NULL and action IS NULL;";

            Set<Long> newRuns = RunInfoUtils.getSetOfRunsFromCertainSelect(select);
            try {
                if (!newRuns.isEmpty()) {
                    RunInfoUtils.fetchRunInfo(newRuns);
                    logger.log(Level.INFO, "List of new runs: " + newRuns + ", nr: " + newRuns.size());
                }

                if (updatedAt == 0) {
                    minTime = ZonedDateTime.now(ZoneId.of("Europe/Zurich"))
                            .minusWeeks(1).toInstant().toEpochMilli();
                    maxTime = currentTime;
                    updatedAt = currentTime;
                    Set<Long> updatedRuns = RunInfoUtils.getLastUpdatedRuns(minTime, maxTime);
                    if (!updatedRuns.isEmpty()) {
                        RunInfoUtils.fetchRunInfo(updatedRuns);
                        logger.log(Level.INFO, "List of updated runs: " + updatedRuns + ", nr: " + updatedRuns.size());
                    }
                }

                if (currentTime - updatedAt >= DAY) {
                    minTime = updatedAt;
                    maxTime = currentTime;
                    updatedAt = currentTime;
                    Set<Long> updatedRuns = RunInfoUtils.getLastUpdatedRuns(minTime, maxTime);
                    if (!updatedRuns.isEmpty()) {
                        RunInfoUtils.fetchRunInfo(updatedRuns);
                        logger.log(Level.INFO, "List of updated runs: " + updatedRuns + ", nr: " + updatedRuns.size());
                    }
                }
            } catch (HandleException he) {
                // todo: send mail - logbook req failed
            }

            synchronized (lock) {
                try {
                    lock.wait(1 * 60 * 1000L);
                } catch (@SuppressWarnings("unused") InterruptedException e) {
                    // ignore
                }
            }
        }
    }

    public void wakeup() {
        synchronized (lock) {
            lock.notifyAll();
        }
    }
}
