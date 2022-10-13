package policymaker;

import java.time.ZoneId;
import java.time.ZonedDateTime;

public class RunInfoThread extends Thread {
    private final long DELTA = 300000; // 5 min in ms
    private static RunInfoThread instance = null;
    private Object lock = new Object();
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
        while (true) {
            long minTime = ZonedDateTime.now(ZoneId.of("Europe/Zurich"))
                    .minusWeeks(1).toInstant().toEpochMilli() / 1000;
            long currentTime = ZonedDateTime.now(ZoneId.of("Europe/Zurich")).toInstant().toEpochMilli();
            long maxTime = (currentTime - DELTA) / 1000;

            RunInfoUtils.logMessage("mintime: " + minTime + ", maxtime: " + maxTime);
            RunInfoUtils.fetchRunInfo(minTime, maxTime);

            synchronized (lock) {
                try {
                    lock.wait(30 * 60 * 1000L);
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
