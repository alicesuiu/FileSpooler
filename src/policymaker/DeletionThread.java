package policymaker;

import alien.catalogue.LFN;
import alien.config.ConfigUtils;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Iterator;
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
        while (true) {
            long lastmodified = ZonedDateTime.now(ZoneId.of("Europe/Zurich"))
                    .minusWeeks(4).toInstant().toEpochMilli() / 1000;

            String select = "select distinct rr.run from rawdata_runs rr left outer join rawdata_runs_action ra on " +
                    "ra.run = rr.run and (action = 'delete' or (action = 'delete replica' and sourcese = 'ALICE::CERN::EOSALICEO2')) where " +
                    "rr.run > 500000 and daq_goodflag = 2 and action is null and lastmodified < " + lastmodified + ";";

            Set<Long> runs = RunInfoUtils.getSetOfRunsFromCertainSelect(select);
            Iterator<Long> it = runs.iterator();
            while (it.hasNext()) {
                Long run = it.next();
                Set<LFN> lfns = DeletionUtils.getLFNsForDeletion(run, null, null, "ALICE::CERN::EOSALICEO2", null);
                if (lfns == null || lfns.isEmpty()) {
                    it.remove();
                    continue;
                }

                RunActionUtils.insertRunAction(run, "delete replica", "all", "Deletion Thread",
                        "todo", lfns.size(), lfns.stream().mapToLong(lfn -> lfn.size).sum(),
                        "ALICE::CERN::EOSALICEO2", null, "Queued");

            }

            if (runs.size() > 0) {
                logger.log(Level.INFO, "List of runs that must be deleted: " + runs + ", nr: " + runs.size());
                String sTo = "Alice Suiu <alicesuiu17@gmail.com>";
                String sFrom = "monalisa@cern.ch";
                String sBody = "Dear colleagues,\n\n";
                sBody += "You can see below information about the runs that need to be deleted!\n\n";
                sBody += "List of runs that must be deleted: " + runs + ", nr: " + runs.size() + "\n";
                sBody += "Storage: ALICE::CERN::EOSALICEO2" + "\n";
                sBody += "Extension: all (.root + .tf)\n\n";
                sBody += "\nBest regards,\nDeletionThread.\n";
                String sSubject = "[DeletionThread] List of runs to delete";
                RunInfoUtils.sendMail(sTo, sFrom, sSubject, sBody);
            }

            synchronized (lock) {
                try {
                    lock.wait(DAY);
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
