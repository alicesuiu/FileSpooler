package policymaker;

import alien.catalogue.LFN;
import lia.Monitor.Store.Fast.DB;

import java.util.*;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class DeletionTask implements Runnable {
    private final Long id;
    private final String table;
    private final File file = new File("/home/monalisa/MLrepository/lib/classes/asuiu/deletionTask.log");

    public DeletionTask(Long id, String table) {
        this.id = id;
        this.table = table;
    }

    public void logToFile(String msg) {
        LocalDateTime currentDate = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        try (FileWriter writeFile = new FileWriter(file.getAbsolutePath(), true)) {
            writeFile.write(currentDate.format(formatter) + " - " + msg + "\n");
        } catch (final IOException e) {
            // todo
        }
    }

    @Override
    public void run() {
        DB db = new DB("select * from " + table + " where id_record = " + id + ";");

        if (!db.moveNext()) {
            logToFile("[WARNING] Select query did not work for id " + id + " and table " + table);
            return;
        }

        Long run = db.getl("run");
        String filter = db.gets("filter");
        Integer percentage = db.geti("percentage", 0);
        String sourcese = db.gets("sourcese", null);
        String targetse = db.gets("targetse", null);
        String status = db.gets("status");
        String action = db.gets("action");
        String log_message = db.gets("log_message");
        String requester = db.gets("source");
        String responsible = db.gets("responsible");
        Integer counter = db.geti("counter");
        Long size = db.getl("size");

        if (percentage == 0 || percentage == 100)
            percentage = null;

        String msg;
        Set<String> replica = RunInfoUtils.getReplicasForRun(run, filter).keySet();
        Map<String, Set<LFN>> seFiles = new HashMap<>();
        Map<String, Long> remainingReplicas = new HashMap<>();
        if (replica.isEmpty()) {
            msg = "All LFNs of type " + filter + " for run " + run + " were already deleted!\n" + log_message;
            status = "Error";
            RunActionUtils.insertRunAction(run, action, filter, requester, msg, counter, size, sourcese,
                    targetse, status, percentage, responsible, id);
            return;
        }

        Set<LFN> lfnsRun = new HashSet<>();
        if (action.equalsIgnoreCase("delete replica") && sourcese != null) {
            ArrayList<String> storages = new ArrayList<>(Arrays.asList(sourcese.split(" ")));
            ArrayList<String> deleteFromSE = new ArrayList<>();

            for (String sStorage : storages) {
                if (replica.contains(sStorage))
                    deleteFromSE.add(sStorage);
            }

            if (deleteFromSE.isEmpty()) {
                msg = "Run " + run + " does not have replicas on the selected storages for "
                        + filter.toUpperCase() + " files\n" + log_message;
                status = "Error";
                RunActionUtils.insertRunAction(run, action, filter, requester, msg, counter, size, sourcese,
                        targetse, status, percentage, responsible, id);
                return;
            }

            for (String storage : deleteFromSE) {
                Set<LFN> lfns = DeletionUtils.getLFNsForDeletion(run, filter, storage, percentage);
                if (!lfns.isEmpty()) {
                    seFiles.put(storage, lfns);
                    lfnsRun.addAll(lfns);
                }
            }

            if (lfnsRun.isEmpty()) {
                msg = "Run " + run + " does not have LFNs on the selected storages\n" + log_message;
                status = "Error";
                RunActionUtils.insertRunAction(run, action, filter, requester, msg, counter, size, sourcese,
                        targetse, status, percentage, responsible, id);
                return;
            }

            if (replica.size() == 1 && !status.equalsIgnoreCase("In progress")) {
                Iterator<String> it = replica.iterator();
                if (it.hasNext()) {
                    String se = it.next();
                    if (deleteFromSE.contains(se)) {
                        msg = "Run " + run + " has a single replica (" + se + ") for " + filter.toUpperCase() + " files!\n" + log_message;
                        status = "Warning";
                        RunActionUtils.insertRunAction(run, action, filter, requester, msg, lfnsRun.size(),
                                lfnsRun.stream().mapToLong(lfn -> lfn.size).sum(), se, targetse, status,
                                percentage, responsible, id);
                        return;
                    }
                }
            }

            sourcese = String.join(" ", new ArrayList<>(seFiles.keySet()));
            remainingReplicas = RunInfoUtils.getReplicasForRun(run, "all");
        } else if (action.equalsIgnoreCase("delete")) {
            lfnsRun = DeletionUtils.getLFNsForDeletion(run, filter, null, percentage);
            if (lfnsRun.isEmpty()) {
                msg = "Run " + run + " was already deleted\n" + log_message;
                status = "Error";
                RunActionUtils.insertRunAction(run, action, filter, requester, msg, counter, size, sourcese,
                        targetse, status, percentage, responsible, id);
                return;
            }
        }

        counter = lfnsRun.size();
        size = lfnsRun.stream().mapToLong(lfn -> lfn.size).sum();

        if (status.equalsIgnoreCase("Inserting")) {
            status = "Queued";
            RunActionUtils.insertRunAction(run, action, filter, requester, log_message, counter, size, sourcese,
                    targetse, status, percentage, responsible, id);
            return;
        }

        if (status.equalsIgnoreCase("In progress")) {
            logToFile("[INFO] Replicas for run " + run + " " + RunInfoUtils.printReplicasForLFNs(remainingReplicas));
            for (Map.Entry<String, Set<LFN>> entry: seFiles.entrySet()) {
                String storage = entry.getKey();
                Set<LFN> lfnsToDelete = entry.getValue();
                logToFile("[INFO] Run " + run + " has " + lfnsToDelete.size() + " files on " + storage);
                DeletionUtils.deleteRun(run, lfnsToDelete, filter, storage, percentage, remainingReplicas, requester, responsible, id);
            }
            String update = "update " + table + " set counter=" + counter + ", size=" + size + ", sourcese='" + sourcese
                    + "' where id_record=" + id + ";";
            logToFile("[INFO] Update for run " + run + " " + update);
            if (!db.syncUpdateQuery(update))
                logToFile("[WARINING] The update action for run " + run + " failed " + db.getLastError());
            return;
        }
    }
}
