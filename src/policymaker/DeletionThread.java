package policymaker;

import alien.config.ConfigUtils;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DeletionThread extends Thread {
    private static DeletionThread instance = null;
    private Object lock = new Object();
    private static Logger logger = ConfigUtils.getLogger(DeletionThread.class.getCanonicalName());
    private final long DAY = 86400 * 1000L;

    private DeletionThread() {
        super(DeletionThread.class.getCanonicalName());
        setDaemon(true);
    }

    public static synchronized DeletionThread getInstance() {
        if (instance == null) {
            instance = new DeletionThread();
            instance.start();
        }
        return instance;
    }

    @Override
    public void run() {
        try {
            while (true) {
                long lastmodified = ZonedDateTime.now(ZoneId.of("Europe/Zurich"))
                        .minusWeeks(4).toInstant().toEpochMilli() / 1000;

                String select = "select rr.run from rawdata_runs rr left outer join rawdata_runs_action ra on " +
                        "ra.run = rr.run and action = 'delete' inner join rawdata_runs_last_action rla on " +
                        "rla.run=rr.run and (ctf like 'ALICE::CERN::EOSALICEO2' and tf like 'ALICE::CERN::EOSALICEO2' " +
                        "and calib like 'ALICE::CERN::EOSALICEO2' and other like 'ALICE::CERN::EOSALICEO2') where " +
                        "rr.run > 500000 and daq_goodflag = 2 and action is null and lastmodified < " + lastmodified + ";";

                Set<Long> runs = RunInfoUtils.getSetOfRunsFromCertainSelect(select);
                DeletionUtils.deleteRunsWithCertainRunQuality(runs,"Test");
            }
        } catch (HandleException e) {
            e.sendMail();
        }
        synchronized (lock) {
            try {
                lock.wait(DAY);
            } catch (@SuppressWarnings("unused") InterruptedException e) {
                // ignore
            }
        }
    }

    public void wakeup() {
        synchronized (lock) {
            lock.notifyAll();
        }
    }
}
