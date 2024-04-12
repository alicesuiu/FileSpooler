package policymaker;

import alien.catalogue.*;
import alien.config.ConfigUtils;
import alien.se.SE;
import alien.se.SEUtils;
import lazyj.DBFunctions;
import lia.Monitor.Store.Fast.DB;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DeletionUtils {
    private static Logger logger = ConfigUtils.getLogger(DeletionUtils.class.getCanonicalName());

    private static List<Long> deletionFailed = new ArrayList<>();

    private static ExecutorService addExecutor = Executors.newFixedThreadPool(1);

    private static ExecutorService delExecutor = Executors.newFixedThreadPool(1);

    public static Future addTask(Long id, String table) {
        return addExecutor.submit(new DeletionTask(id, table));
    }

    public static Future delTask(Long id, String table) {
        return delExecutor.submit(new DeletionTask(id, table));
    }

    public static Set<LFN> getLFNsForDeletion(String startDir, String storage, Integer limit) {
        Set<LFN> lfns = new HashSet<>();

        if (startDir == null)
            return lfns;

        lfns = RunInfoUtils.getLFNsFromRawdata(startDir);

        if (storage != null && !storage.isEmpty())
            RunInfoUtils.getLfnsFromCertainStorage(lfns, storage);

        if (limit != null && limit > 0 && limit <= 100) {
            lfns = RunInfoUtils.getFirstXLfns(lfns, limit);
            logger.log(Level.INFO, "Lfns list size after apply limit " + limit + "%: " + lfns.size());
        }

        if (!lfns.isEmpty())
            logger.log(Level.INFO, "LFNs to be deleted: " + lfns.size());
        return lfns;
    }

    public static Set<LFN> getLFNsForDeletion(Long run, String extension, String storage, Integer limit) {
        Set<LFN> lfns = RunInfoUtils.getLFNsFromRawdataDetails(run, extension);

        if (storage != null && !storage.isEmpty())
            RunInfoUtils.getLfnsFromCertainStorage(lfns, storage);

        if (limit != null && limit > 0 && limit <= 100) {
            lfns = RunInfoUtils.getFirstXLfns(lfns, limit);
            logger.log(Level.INFO, "Lfns list size after apply limit " + limit + "%: " + lfns.size());
        }

        if (!lfns.isEmpty())
            logger.log(Level.INFO, "LFNs to be deleted: " + lfns.size());
        return lfns;
    }

    public static void deleteRun(Long run, Set<LFN> lfns, String extension, String storage, Integer limit,
                                 Map<String, Long> seFiles, String requester, String responsible,
                                 Long id_record) {
        DB db = new DB();
        SE se = null;
        String action = "", sourcese = null;

        if (storage != null && !storage.isEmpty())
            se = SEUtils.getSE(storage);

        Iterator<LFN> lfnsIterator = lfns.iterator();
        boolean status = true, last_replica;
        long success_deleted = 0;
        while (lfnsIterator.hasNext()) {
            LFN l = lfnsIterator.next();
            last_replica = false;
            if (storage != null) {
                GUID g = GUIDUtils.getGUID(l);
                if (g.hasReplica(se)) {
                    if (g.removePFN(se, true) == null) {
                        status = false;
                        logger.log(Level.WARNING, "The deletion of the " + l.getName() + " failed");
                    } else {
                        if (g.getPFNs().isEmpty())
                            last_replica = true;
                        else
                            success_deleted += 1;
                    }
                }
            }

            if (storage == null || last_replica) {
                if (!l.delete(true, false)) {
                    status = false;
                    logger.log(Level.WARNING, "The deletion of the " + l.getName() + " failed");
                } else {
                    success_deleted += 1;
                    String update = "update rawdata set status='deleted'" +
                            " where lfn = '" + l.getCanonicalName() + "';";
                    if (!db.syncUpdateQuery(update)) {
                        logger.log(Level.WARNING, "Status update in rawdata failed for run: " + run + " " + db.getLastError());
                    }
                }
            }
        }

        if (status && success_deleted == lfns.size()) {
            String select = "select daq_goodflag from rawdata_runs where run = " + run + ";";
            db.query(select);
            int iDaqGoodFlag = db.geti("daq_goodflag", -1);
            String sDaqGoodFlag = RunInfoUtils.getRunQuality(iDaqGoodFlag);
            if (sDaqGoodFlag == null)
                sDaqGoodFlag = "no logbook entry for this";

            if (se != null) {
                seFiles.put(se.seName, seFiles.get(se.seName) - lfns.size());
                if (seFiles.get(se.seName) <= 0)
                    seFiles.remove(se.seName);
            }

            if (storage == null || seFiles.isEmpty())
                action = "delete";
            else {
                action = "delete replica";
                sourcese = storage;
            }

            String log_message = sDaqGoodFlag + " run; deleted " + success_deleted + " files";
            if (!seFiles.isEmpty()) {
                log_message += "; remaining files";
                for (Map.Entry<String, Long> entry : seFiles.entrySet()) {
                    log_message += " " + entry.getKey() + " : " + entry.getValue();
                }
            }

            if (id_record != null) {
                select = "select log_message from rawdata_runs_action where id_record = " + id_record + ";";
                db.query(select);
                String prev_log_message = db.gets("log_message", "");
                if (!prev_log_message.isBlank())
                    log_message += "\n" + prev_log_message;
            }

            String msg = run + "," + action + "," + extension + "," + sourcese + ","
                    + log_message + "," + lfns.size() + "," + lfns.stream().mapToLong(lfn -> lfn.size).sum();

            long ret = RunActionUtils.insertRunAction(run, action, extension, requester, log_message, lfns.size(),
                    lfns.stream().mapToLong(lfn -> lfn.size).sum(), sourcese, null, "Done", limit, responsible, id_record);

            if (ret > 0) {
                RunActionUtils.retrofitRawdataRunsLastAction(run);

                if (action.equalsIgnoreCase("delete"))
                    updateFileCounters(run, lfns);
                logger.log(Level.INFO, "Successful deletion: " + msg);
            }
        } else {
            logger.log(Level.WARNING, "The deletion of the " + run + " run failed.");
            if (id_record != null) {
                String query = "update rawdata_runs_action set status = 'Error', log_message = E'deletion failed\n' || log_message"
                        + " where id_record = " + id_record + ";";
                db.query(query);
            }
            deletionFailed.add(run);
        }
    }

    public static void deleteRuns(Set<Long> runs, String extension, String storage, Integer limit,
                                  String requester, String responsible, Long id_record) {
        deletionFailed.clear();
        logger.log(Level.INFO, "List of runs that must be deleted: " + runs + ", nr: " + runs.size());

        for (Long run : runs) {
            Map<String, Long> seFiles = new HashMap<>();
            if (storage != null)
                seFiles = RunInfoUtils.getReplicasForRun(run, "all");

            Set<LFN> lfnsToDelete = getLFNsForDeletion(run, extension, storage, limit);
            if (lfnsToDelete != null && !lfnsToDelete.isEmpty())
                deleteRun(run, lfnsToDelete, extension, storage, limit, seFiles, requester, responsible, id_record);
        }

        if (!deletionFailed.isEmpty()) {
            logger.log(Level.WARNING, "Runs with failed deletion: " + deletionFailed + ", nr: " + deletionFailed.size());
        }
    }

    private static void updateFileCounters(Long run, Set<LFN> lfns) {
        DB db = new DB();
        String query = "select partition from rawdata_runs where run = " + run;
        db.query(query);
        String partition = db.gets("partition", null);

        Map<String, Pair<Integer, Long>> initialCounters = RunInfoUtils.getLFNsType(run);
        Map<String, Pair<Integer, Long>> currentCounters = RunInfoUtils.getLFNsType(lfns);
        Integer remainingTfsCnt = Math.abs(initialCounters.get("tf").getFirst() - currentCounters.get("tf").getFirst());
        Long remainingTfsSize = Math.abs(initialCounters.get("tf").getSecond() - currentCounters.get("tf").getSecond());
        Integer remainingCtfsCnt = Math.abs(initialCounters.get("ctf").getFirst() - currentCounters.get("ctf").getFirst());
        Long remainingCtfsSize = Math.abs(initialCounters.get("ctf").getSecond() - currentCounters.get("ctf").getSecond());
        Integer remainingOtherCnt = Math.abs(initialCounters.get("other").getFirst() - currentCounters.get("other").getFirst());
        Long remainingOtherSize = Math.abs(initialCounters.get("other").getSecond() - currentCounters.get("other").getSecond());

        Map<String, Object> values = new HashMap<>();
        values.put("tf_file_count", remainingTfsCnt);
        values.put("tf_file_size", remainingTfsSize);
        values.put("ctf_file_count", remainingCtfsCnt);
        values.put("ctf_file_size", remainingCtfsSize);
        values.put("other_file_count", remainingOtherCnt);
        values.put("other_file_size", remainingOtherSize);
        values.put("run", run);
        values.put("partition", partition);

        String msg = "TFs (" + remainingTfsCnt + "," + remainingTfsSize + ")" + ", CTFs (" + remainingCtfsCnt + "," +
                remainingCtfsSize + "), Other (" + remainingOtherCnt + "," + remainingOtherSize + ")";
        logger.log(Level.INFO, "Update counters for run " + run + ": " + msg);

        query = DBFunctions.composeUpsert("rawdata_runs", values, Set.of("run", "partition"));
        db.query(query);
    }

    public static boolean hasDuplicates(Long run, String action, String filter, String sourcese, Integer percentage) {
        DB db = new DB();
        boolean bFilter = filter.equalsIgnoreCase("ctf") ||
                filter.equalsIgnoreCase("tf") || filter.equalsIgnoreCase("other");

        /*
         * record already inserted: action = delete, filter = all
         * possible duplicates:
         *   action = delete replica, filter = {all/ctf/tf/other}
         *   action = delete, filter = {ctf/tf/other}
         */

        String select = "select count(1) as cnt from rawdata_runs_action where run = " + run
                + " and (status = 'Done' or status = 'In progress' or status = 'Queued' or status = 'Warning')"
                + " and action = 'delete' and filter = 'all';";
        db.query(select);
        if (db.geti("cnt") > 0 &&
                ((action.equalsIgnoreCase("delete") && bFilter) ||
                        (action.equalsIgnoreCase("delete replica") &&
                                (filter.equalsIgnoreCase("all") ||  bFilter)))) {
            return true;
        }

        /*
         * record already inserted: action = delete, filter = ctf
         * possible duplicates:
         *   action = delete replica, filter = ctf
         */

        select = "select count(1) as cnt from rawdata_runs_action where run = " + run
                + " and (status = 'Done' or status = 'In progress' or status = 'Queued' or status = 'Warning')"
                + " and action = 'delete' and filter = 'ctf';";
        db.query(select);
        if (db.geti("cnt") > 0 && action.equalsIgnoreCase("delete replica") && filter.equalsIgnoreCase("ctf"))
            return true;

        /*
         * record already inserted: action = delete, filter = tf
         * possible duplicates:
         *   action = delete replica, filter = tf
         */

        select = "select count(1) as cnt from rawdata_runs_action where run = " + run
                + " and (status = 'Done' or status = 'In progress' or status = 'Queued' or status = 'Warning')"
                + " and action = 'delete' and filter = 'tf';";
        db.query(select);
        if (db.geti("cnt") > 0 && action.equalsIgnoreCase("delete replica") && filter.equalsIgnoreCase("tf"))
            return true;

        /*
         * record already inserted: action = delete, filter = other
         * possible duplicates:
         *   action = delete replica, filter = other
         */

        select = "select count(1) as cnt from rawdata_runs_action where run = " + run
                + " and (status = 'Done' or status = 'In progress' or status = 'Queued' or status = 'Warning')"
                + " and action = 'delete' and filter = 'other';";
        db.query(select);
        if (db.geti("cnt") > 0 && action.equalsIgnoreCase("delete replica") && filter.equalsIgnoreCase("other"))
            return true;

        /*
         * record already inserted: action = delete replica, filter = all
         * possible duplicates:
         *   action = delete replica, filter = {ctf/tf/other}
         */

        select = "select count(1) as cnt, sourcese from rawdata_runs_action where run = " + run
                + " and (status = 'Done' or status = 'In progress' or status = 'Queued' or status = 'Warning')"
                + " and action = 'delete replica' and filter = 'all' group by sourcese;";
        db.query(select);
        if (db.geti("cnt") > 0 && action.equalsIgnoreCase("delete replica") && bFilter) {
            while (db.moveNext()) {
                String insertedSEs = db.gets("sourcese", "");
                String currentSourcese = sourcese;
                if (sourcese == null)
                    currentSourcese = "";
                if (insertedSEs.contains(currentSourcese) || currentSourcese.contains(insertedSEs) || insertedSEs.equalsIgnoreCase(currentSourcese))
                    return true;
            }
        }

        /*
         * record already inserted that has same values for action, filter, sourcese, percentage
         */
        select = "select count(1) as cnt from rawdata_runs_action where run = " + run +
                " and action = '" + action + "' and filter = '" + filter + "'";
        if (sourcese == null)
            select += " and sourcese is null";
        else
            select += " and sourcese = '" + sourcese + "'";

        if (percentage == null)
            select += " and percentage is null";
        else
            select += " and percentage = " + percentage;
        db.query(select);
        if (db.geti("cnt") > 0)
            return true;

        return false;
    }
}