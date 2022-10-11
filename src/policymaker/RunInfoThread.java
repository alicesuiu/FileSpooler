package policymaker;

import java.time.ZoneId;
import java.time.ZonedDateTime;

public class RunInfoThread implements Runnable {
    private final long DELTA = 300000; // 5 min in ms
    @Override
    public void run() {
        final long minTime = ZonedDateTime.now(ZoneId.of("Europe/Zurich"))
                .minusWeeks(1).toInstant().toEpochMilli() / 1000;
        final long currentTime = ZonedDateTime.now(ZoneId.of("Europe/Zurich")).toInstant().toEpochMilli();
        final long maxTime = (currentTime - DELTA) / 1000;

        RunInfoUtils.logMessage("mintime: " + minTime + ", maxtime: " + maxTime);
        RunInfoUtils.setRunInfo(minTime, maxTime);
    }
}
