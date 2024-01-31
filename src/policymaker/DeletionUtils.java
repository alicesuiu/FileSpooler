package policymaker;

import alien.catalogue.*;
import alien.config.ConfigUtils;
import alien.se.SE;
import alien.se.SEUtils;
import lazyj.DBFunctions;
import lia.Monitor.Store.Fast.DB;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DeletionUtils {
    private static Logger logger = ConfigUtils.getLogger(DeletionUtils.class.getCanonicalName());
    private static List<Long> deletionFailed = new ArrayList<>();

    public static Set<LFN> getLFNsForDeletion(String startDir, String extension, String storage, Integer limit) {
        Set<LFN> lfns = new HashSet<>();

        if (startDir == null)
            return lfns;

        lfns = RunInfoUtils.getLFNsFromRawdata(startDir);
        if (extension != null && !extension.isEmpty())
            RunInfoUtils.getLfnsWithCertainExtension(lfns, extension);
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

    public static void filterLFNs(Set<LFN> lfns, String extension, String storage, Integer limit) {
        if (extension != null && !extension.isEmpty())
            RunInfoUtils.getLfnsWithCertainExtension(lfns, extension);
        if (storage != null && !storage.isEmpty())
            RunInfoUtils.getLfnsFromCertainStorage(lfns, storage);
        if (limit != null && limit > 0 && limit <= 100) {
            lfns = RunInfoUtils.getFirstXLfns(lfns, limit);
            logger.log(Level.INFO, "Lfns list size after apply limit " + limit + "%: " + lfns.size());
        }
        if (!lfns.isEmpty())
            logger.log(Level.INFO, "LFNs to be deleted: " + lfns.size());
    }

    private static void deleteRun(Long run, Set<LFN> lfns, String extension, String storage, Integer limit, Map<String, Long> seFiles) {
        DB db = new DB();
        SE se = null;
        String action = "", sourcese = null, filter;

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

            if (extension == null)
                filter = "all";
            else if (extension.equals(".tf"))
                filter = "tf";
            else if (extension.equals(".root"))
                filter = "ctf";
            else
                filter = extension;

            //logger.log(Level.INFO, "Insert: " + run + "," + action + "," + filter + "," + sourcese + "," + log_message + "," + lfns.size() + "," + lfns.stream().mapToLong(lfn -> lfn.size).sum());

            int ret = RunActionUtils.insertRunAction(run, action, filter, "Deletion Thread", log_message, lfns.size(),
                    lfns.stream().mapToLong(lfn -> lfn.size).sum(), sourcese, null, "Done", limit);

            if (ret >= 0) {
                RunActionUtils.retrofitRawdataRunsLastAction(run);
                updateFileCounters(run, lfns);
                logger.log(Level.INFO, "Successful deletion of the " + run + " run");
            }
        } else {
            logger.log(Level.WARNING, "The deletion of the " + run + " run failed.");
            deletionFailed.add(run);
        }
    }

    public static void deleteRuns(Set<Long> runs, String extension, String storage, Integer limit) {
        deletionFailed.clear();
        logger.log(Level.INFO, "List of runs that must be deleted: " + runs + ", nr: " + runs.size());

        for (Long run : runs) {
            Set<LFN> lfnsToDelete = RunInfoUtils.getLFNsFromRawdataDetails(run);
            Map<String, Long> seFiles = new HashMap<>();
            if (storage != null) {
                seFiles = RunInfoUtils.getReplicasForLFNs(lfnsToDelete);
                logger.log(Level.INFO, "Replicas for run " + run + RunInfoUtils.printReplicasForLFNs(seFiles));
            }
            filterLFNs(lfnsToDelete, extension, storage, limit);
            if (!lfnsToDelete.isEmpty())
                deleteRun(run, lfnsToDelete, extension, storage, limit, seFiles);
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
    
    public static void deleteRunsWithCertainRunQuality(Set<Long> runs, String runQuality, String extension, String storage, Integer limit) throws HandleException {
        int daqGoodFlag = RunInfoUtils.getDaqGoodFlag(runQuality);
        if (Arrays.asList(0, 1, 2).contains(daqGoodFlag)) {
            Set<Long> updatedRuns = new HashSet<>();
            Iterator<Long> runsIterator = runs.iterator();
            while (runsIterator.hasNext()) {
                Long run = runsIterator.next();
                Set<RunInfo> runInfos = RunInfoUtils.getRunInfoFromLogBook(String.valueOf(run));
                if (!runInfos.isEmpty()) {
                    RunInfo runInfo = runInfos.iterator().next();
                    if (!runInfo.getRunQuality().equalsIgnoreCase(runQuality)) {
                        logger.log(Level.WARNING, "The run quality for run " + run
                                + "has been changed from " + runQuality + " to " + runInfo.getRunQuality());
                        updatedRuns.add(run);
                        runsIterator.remove();
                    }
                }
            }

            if (!updatedRuns.isEmpty()) {
                logger.log(Level.WARNING, "Runs with different info in Logbook than database: " + updatedRuns + ", nr: " + updatedRuns.size());
                RunInfoUtils.fetchRunInfo(updatedRuns);
            }

            //deleteRuns(runs, true, extension, storage, limit);
        } else {
            logger.log(Level.WARNING, "The received run quality " + runQuality + " is invalid.");
        }
    }
}
