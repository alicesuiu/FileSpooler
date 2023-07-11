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
    private static Map<String, List<String>> messages = new HashMap<>();
    private static final int RUN_DELETION_FAILED = -1;

    public static Map<String, List<String>> getMessages() {
        return messages;
    }

    private static int deleteRun(Long run, boolean logbookEntry, String extension, String storage, Integer limit) {
        DB db = new DB();
        Map<String, Object> values = new HashMap<>();
        String select = RunInfoUtils.getCollectionPathQuery(run, logbookEntry);
        String action = "", sourcese = "";
        SE se = null;
        List<String> infoLevel = messages.computeIfAbsent("Info", (k) -> new ArrayList<>());
        List<String> warningLevel = messages.computeIfAbsent("Warning", (k) -> new ArrayList<>());

        db.query(select);
        while (db.moveNext()) {
            String collection_path = db.gets("collection_path");
            if (collection_path.isEmpty() || collection_path.isBlank())
                continue;
            logger.log(Level.INFO, collection_path);
            infoLevel.add("Collection path: " + collection_path);
            Set<LFN> lfns = RunInfoUtils.getLFNsFromCollection(collection_path);

            if (extension != null && extension.length() > 0) {
                RunInfoUtils.getLfnsWithCertainExtension(lfns, extension);
            } else {
                RunInfoUtils.getAllLFNs(lfns);
            }

            Map<String, Long> countSizePair = RunInfoUtils.getCountSizeRun(run);
            long count = countSizePair.get("count");
            long size = countSizePair.get("size");
            if (count <= 0 || size <= 0) {
                logger.log(Level.WARNING, "The " + run + " run was deleted");
                warningLevel.add("The " + run + " run was already deleted");
                continue;
            }

            if (extension == null && count != lfns.size()) {
                logger.log(Level.WARNING, "The number of LFNs from rawdata_runs (" + count
                        + ") for run " + run + " is different than the one in the LFNs list (" + lfns.size() + ")");
                size = lfns.stream().mapToLong(lfn -> lfn.size).sum();
                warningLevel.add("The number of LFNs from rawdata_runs (" + count
                        + ") for run " + run + " is different than the one in the LFNs list (" + lfns.size() + ")");
                //continue;
            }

            if (storage != null && storage.length() > 0) {
                RunInfoUtils.getLfnsFromCertainStorage(lfns, storage);
                se = SEUtils.getSE(storage);
            }

            if (limit != null && limit > 0) {
                lfns = RunInfoUtils.getFirstXLfns(lfns, limit);
                size = lfns.stream().mapToLong(lfn -> lfn.size).sum();
            }

            Iterator<LFN> lfnsIterator = lfns.iterator();
            boolean status = true, last_replica;
            long success_deleted = 0;
            Map<String, Long> seFiles = new HashMap<>();
            while (lfnsIterator.hasNext()) {
                LFN l = lfnsIterator.next();
                last_replica = false;
                if (storage != null) {
                    GUID g = GUIDUtils.getGUID(l);
                    if (g.hasReplica(se)) {
                        if (g.removePFN(se, true) == null) {
                            status = false;
                            logger.log(Level.WARNING, "The deletion of the " + l.getName() + " failed");
                            warningLevel.add("The deletion of the " + l.getName() + " failed");
                        } else {
                            Set<PFN> pfns = g.getPFNs();
                            if (pfns.size() == 0) {
                                last_replica = true;
                            } else {
                                success_deleted += 1;
                                for (PFN pfn : pfns) {
                                    String seName = pfn.getSE().seName;
                                    if (se != null && !seName.equalsIgnoreCase(se.seName)) {
                                        Long cnt = seFiles.computeIfAbsent(seName, (k) -> 0L) + 1;
                                        seFiles.put(seName, cnt);
                                    }
                                }
                            }
                        }
                    }
                }

                if (storage == null || last_replica) {
                    if (!l.delete(true, false)) {
                        status = false;
                        logger.log(Level.WARNING, "The deletion of the " + l.getName() + " failed");
                        warningLevel.add("The deletion of the " + l.getName() + " failed");
                    } else {
                        success_deleted += 1;
                        String update = "update rawdata set status='deleted'" +
                                " where lfn = '" + l.getCanonicalName() + "';";
                        if (!db.syncUpdateQuery(update)) {
                            logger.log(Level.WARNING, "Status update in rawdata failed for run: " + run + " " + db.getLastError());
                            warningLevel.add("Status update in rawdata failed for run: " + run + " " + db.getLastError());
                        }
                    }
                }
            }

            if (status && success_deleted == lfns.size()) {
                select = "select daq_goodflag from rawdata_runs where run = " + run + ";";
                db.query(select);
                int iDaqGoodFlag = db.geti("daq_goodflag", -1);
                String sDaqGoodFlag = RunInfoUtils.getRunQuality(iDaqGoodFlag);
                if (sDaqGoodFlag == null)
                    sDaqGoodFlag = "no logbook entry for this";

                if (extension == null)
                    values.put("filter", "all");
                else if (extension.equals(".tf"))
                    values.put("filter", "tf");
                else if (extension.equals(".root"))
                    values.put("filter", "ctf");
                else
                    values.put("filter", extension);

                values.put("counter", lfns.size());
                values.put("size", size);

                if (storage == null || seFiles.isEmpty())
                    action = "delete";
                else {
                    action = "delete replica";
                    sourcese = storage;
                    values.put("sourcese", sourcese);
                }
                values.put("action", action);
                values.put("run", run);
                values.put("source", "Deletion Thread");
                String log_message = sDaqGoodFlag + " run; deleted " + success_deleted + " files";
                if (!seFiles.isEmpty()) {
                    log_message += "; remaining files";
                    for (Map.Entry<String, Long> entry : seFiles.entrySet()) {
                        log_message += " " + entry.getKey() + " : " + entry.getValue();
                    }
                }
                values.put("log_message", log_message);

                String insert = DBFunctions.composeInsert("rawdata_runs_action", values);
                logger.log(Level.INFO, insert);
                infoLevel.add("Update action: " + insert);
                if (!db.query(insert)) {
                    logger.log(Level.WARNING, "Insert in rawdata_runs_action failed for run: " + run + " " + db.getLastError());
                    warningLevel.add("Insert in rawdata_runs_action failed for run: " + run + " " + db.getLastError());
                } else {
                    RunActionUtils.retrofitRawdataRunsLastAction(run);
                    infoLevel.add("Successful deletion of the " + run + " run");
                }
            } else {
                logger.log(Level.WARNING, "The deletion of the " + run + " run failed.");
                warningLevel.add("The deletion of the " + run + " run failed.");
                messages.put("Info", infoLevel);
                messages.put("Warning", warningLevel);
                return RUN_DELETION_FAILED;
            }
        }
        messages.put("Info", infoLevel);
        messages.put("Warning", warningLevel);
        return 0;
    }

    private static void deleteCheckStatus(Set<Long> runs, boolean logbookEntry, String extension, String storage, Integer limit) {
        List<Long> deletionFailed = new ArrayList<>();

        logger.log(Level.INFO, "List of runs that must be deleted: " + runs + ", nr: " + runs.size());
        for (Long run : runs) {
            int status = deleteRun(run, logbookEntry, extension, storage, limit);
           if (status == RUN_DELETION_FAILED)
                deletionFailed.add(run);
            else
                logger.log(Level.INFO, "Successful deletion of the " + run + " run");
        }

        if (!deletionFailed.isEmpty())
            logger.log(Level.WARNING, "Runs with failed deletion: " + deletionFailed + ", nr: " + deletionFailed.size());
    }

    public static void deleteRunsNoLogbookEntry(Set<Long> runs, String extension, String storage, Integer limit) {
        deleteCheckStatus(runs, false, extension, storage, limit);
    }

    public static void deleteRunsWithLogbookEntry(Set<Long> runs, String extension, String storage, Integer limit) {
        deleteCheckStatus(runs, true, extension, storage, limit);
    }

    public static void deleteRunsWithCertainRunQuality(Set<Long> runs, String runQuality) throws HandleException {
        int daqGoodFlag = RunInfoUtils.getDaqGoodFlag(runQuality);
        if (Arrays.asList(0, 1, 2).contains(daqGoodFlag)) {
            Set<Long> updatedRuns = new HashSet<>();
            logger.log(Level.INFO, "List of runs that must be deleted: " + runs + ", nr: " + runs.size());
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
                //RunInfoUtils.fetchRunInfo(updatedRuns);
            }

            //RunInfoUtils.deleteRunsWithLogbookEntry(runs);
        } else {
            logger.log(Level.WARNING, "The received run quality " + runQuality + " is invalid.");
        }
    }
}
