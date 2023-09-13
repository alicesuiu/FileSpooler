package policymaker;

import alien.catalogue.*;
import alien.config.ConfigUtils;
import alien.se.SE;
import alien.se.SEUtils;
import lia.Monitor.Store.Fast.DB;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DeletionUtils {
    private static Logger logger = ConfigUtils.getLogger(DeletionUtils.class.getCanonicalName());
    private static Map<Long, Pair<List<String>, List<String>>> messages = new HashMap<>();
    private static Map<String, List<String>> globalMessages = new HashMap<>();
    private static List<Long> deletionFailed = new ArrayList<>();

    public static Map<String, List<String>> getGlobalMessages() {
        return globalMessages;
    }

    public static List<String> getGlobalInfoLevelMessages() {
        return globalMessages.computeIfAbsent("Info", (k) -> new ArrayList<>());
    }

    public static List<String> getGlobalWarningLevelMessages() {
        return globalMessages.computeIfAbsent("Warning", (k) -> new ArrayList<>());
    }

    public static Pair<List<String>, List<String>> getRunMessages(Long run) {
        return messages.computeIfAbsent(run,
                (k) -> new Pair<>(new ArrayList<>(), new ArrayList<>()));
    }
    public static List<String> getInfoLevelMessagesForRun(Long run) {
        return getRunMessages(run).getFirst();
    }

    public static List<String> getWarningLevelMessagesForRun(Long run) {
        return getRunMessages(run).getSecond();
    }

    public static Set<LFN> getLFNsForDeletion(Long run, Boolean logbookEntry, String extension, String storage, Integer limit) {
        List<String> infoLevel = getInfoLevelMessagesForRun(run);
        List<String> warningLevel = getWarningLevelMessagesForRun(run);
        Set<LFN> lfns;

        if (logbookEntry == null)
            lfns = RunInfoUtils.getLFNsFromRawdataDetails(run);
        else
            lfns = RunInfoUtils.getLFNsFromCollection(run, logbookEntry);

        if (extension != null && extension.length() > 0) {
            RunInfoUtils.getLfnsWithCertainExtension(lfns, extension);
            infoLevel.add("LFNs list size for extension (" + extension + "): " + lfns.size());
        } else {
            RunInfoUtils.getAllLFNs(lfns);
            infoLevel.add("LFNs list size (.root + .tf) : " + lfns.size());
        }

        Map<String, Long> countSizePair = RunInfoUtils.getCountSizeRun(run);
        long count = countSizePair.get("count");
        long size = countSizePair.get("size");
        if (count <= 0 || size <= 0) {
            logger.log(Level.WARNING, "The " + run + " run was deleted");
            warningLevel.add("The " + run + " run was already deleted");
            return null;
        }

        if (extension == null && count != lfns.size()) {
            logger.log(Level.WARNING, "The number of LFNs from rawdata_runs (" + count
                    + ") for run " + run + " is different than the one in the LFNs list (" + lfns.size() + ")");
            warningLevel.add("The number of LFNs from rawdata_runs (" + count
                    + ") for run " + run + " is different than the one in the LFNs list (" + lfns.size() + ")");
        }

        if (storage != null && storage.length() > 0) {
            RunInfoUtils.getLfnsFromCertainStorage(lfns, storage);
            infoLevel.add("LFNs list size from storage: " + lfns.size());
        }

        if (limit != null && limit > 0)
            lfns = RunInfoUtils.getFirstXLfns(lfns, limit);

        if (lfns.size() > 0) {
            logger.log(Level.INFO, "LFNs to be deleted: " + lfns.size());
            infoLevel.add("LFNs to be deleted: " + lfns.size());
        }
        return lfns;
    }

    private static void deleteRun(Long run, Set<LFN> lfns, String extension, String storage) {
        DB db = new DB();
        SE se = null;
        String action = "", sourcese = null, filter;
        List<String> infoLevel = getInfoLevelMessagesForRun(run);
        List<String> warningLevel = getWarningLevelMessagesForRun(run);

        if (storage != null && storage.length() > 0)
            se = SEUtils.getSE(storage);

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
            String select = "select daq_goodflag from rawdata_runs where run = " + run + ";";
            db.query(select);
            int iDaqGoodFlag = db.geti("daq_goodflag", -1);
            String sDaqGoodFlag = RunInfoUtils.getRunQuality(iDaqGoodFlag);
            if (sDaqGoodFlag == null)
                sDaqGoodFlag = "no logbook entry for this";

            if (extension == null)
                filter = "all";
            else if (extension.equals(".tf"))
                filter = "tf";
            else if (extension.equals(".root"))
                filter = "ctf";
            else
                filter = extension;

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

            //logger.log(Level.INFO, "Insert: " + run + "," + action + "," + filter + "," + sourcese + "," + log_message + "," + lfns.size() + "," + lfns.stream().mapToLong(lfn -> lfn.size).sum());

            int ret = RunActionUtils.insertRunAction(run, action, filter, "Deletion Thread", log_message, lfns.size(),
                    lfns.stream().mapToLong(lfn -> lfn.size).sum(), sourcese, null, "Done");

            if (ret >= 0) {
                RunActionUtils.retrofitRawdataRunsLastAction(run);
                infoLevel.add("Successful deletion of the " + run + " run");
                logger.log(Level.INFO, "Successful deletion of the " + run + " run");
            }
        } else {
            logger.log(Level.WARNING, "The deletion of the " + run + " run failed.");
            warningLevel.add("The deletion of the " + run + " run failed.");
            deletionFailed.add(run);
        }
    }

    public static void deleteRuns(Set<Long> runs, Boolean logbookEntry, String extension, String storage, Integer limit) {
        messages.clear();
        deletionFailed.clear();
        globalMessages.clear();

        List<String> infoLevel = getGlobalInfoLevelMessages();
        List<String> warningLevel = getGlobalWarningLevelMessages();

        logger.log(Level.INFO, "List of runs that must be deleted: " + runs + ", nr: " + runs.size());
        infoLevel.add("List of runs that must be deleted: " + runs + ", nr: " + runs.size());

        for (Long run : runs) {
            Set<LFN> lfnsToDelete = getLFNsForDeletion(run, logbookEntry, extension, storage, limit);
            if (lfnsToDelete != null && !lfnsToDelete.isEmpty())
                deleteRun(run, lfnsToDelete, extension, storage);
        }

        if (!deletionFailed.isEmpty()) {
            logger.log(Level.WARNING, "Runs with failed deletion: " + deletionFailed + ", nr: " + deletionFailed.size());
            warningLevel.add("Runs with failed deletion: " + deletionFailed + ", nr: " + deletionFailed.size());
        }

        globalMessages.put("Info", infoLevel);
        globalMessages.put("Warning", warningLevel);
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

    public static String printMessages(Long run) {
        String msg = "";
        List<String> infoLevel = getInfoLevelMessagesForRun(run);
        List<String> warningLevel = getWarningLevelMessagesForRun(run);
        int i;

        msg += "[INFO] Run " + run + ":\n";
        for (i = 0; i < infoLevel.size(); i++)
            msg += "[INFO] " + infoLevel.get(i) + "\n";

        for (i = 0; i < warningLevel.size(); i++)
            msg += "[WARNING] " + warningLevel.get(i) + "\n";

        return msg;
    }

    public static String printMessages(Set<Long> runs, boolean printLFNs, boolean logbookEntry, boolean printReplicas, String extension, String storage, Integer limit) {
        String msg = "", intro = "";
        Iterator<Long> runsIterator = runs.iterator();
        while (runsIterator.hasNext()) {
            Long run = runsIterator.next();
            messages.remove(run);
            Set<LFN> lfnsToDelete = getLFNsForDeletion(run, logbookEntry, extension, storage, limit);
            if (lfnsToDelete.size() > 0) {
                msg += printMessages(run);
                if (printLFNs) {
                    msg += "[INFO] LFNs list: ";
                    for (LFN l : lfnsToDelete)
                        msg += l.getCanonicalName() + " ";
                    msg += "\n";
                }
                if (printReplicas) {
                    Map<String, Long> seFiles = RunInfoUtils.getReplicasForRun(run, logbookEntry, extension);
                    if (seFiles != null && !seFiles.isEmpty()) {
                        msg += "[INFO] List of replicas: ";
                        msg += RunInfoUtils.printReplicasForLFNs(seFiles);
                        msg += "\n";
                    }
                }
                msg += "\n";
            } else {
                runsIterator.remove();
            }
        }
        intro += "[INFO] List of runs that must be deleted: " + runs + ", nr: " + runs.size() + "\n";
        intro += "[INFO] Storage: " + storage + "\n";
        intro += "[INFO] Extension: " + (extension == null ? "all (.root + .tf)" : extension) + "\n\n";
        intro += msg;
        return intro;
    }
}
