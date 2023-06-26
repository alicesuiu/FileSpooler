package policymaker;

import alien.catalogue.LFN;
import alien.config.ConfigUtils;
import alien.managers.TransferManager;
import alien.se.SEUtils;
import lazyj.DBFunctions;
import lia.Monitor.Store.Fast.DB;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MoveUtils {
    private static Logger logger = ConfigUtils.getLogger(MoveUtils.class.getCanonicalName());
    private static Set<LFN> moveRun(Long run, boolean logbookEntry) {
        DB db = new DB();
        String select = RunInfoUtils.getCollectionPathQuery(run, logbookEntry);
        Set<LFN> lfnsToTransfer = new HashSet<>();
        db.query(select);
        while (db.moveNext()) {
            String collectionPath = db.gets("collection_path");
            if (collectionPath.isEmpty() || collectionPath.isBlank())
                continue;
            logger.log(Level.INFO, collectionPath);
            Set<LFN> lfns = RunInfoUtils.getLFNsFromCollection(collectionPath);
            if (lfns.size() == 0) {
                logger.log(Level.WARNING, "The " + run + " run with collection path " + collectionPath + " was deleted");
                continue;
            }
            lfnsToTransfer.addAll(lfns);
        }
        return lfnsToTransfer;
    }

    public static void moveRuns(Set<Long> runs, String targetSE, String sourceSE, boolean logbookEntry, String logMessage, String extension, String storage) {
        //int transferId = TransferManager.getTransferId(SEUtils.getSE(targetSE), logMessage, sourceSE);
        DB db = new DB();

        for (Long run : runs) {
            Set<LFN> lfns = moveRun(run, logbookEntry);
            if (extension != null && extension.length() > 0) {
                RunInfoUtils.getLfnsWithCertainExtension(lfns, extension);
            } else {
                RunInfoUtils.getAllLFNs(lfns);
            }

            if (storage != null && storage.length() > 0) {
                RunInfoUtils.getLfnsFromCertainStorage(lfns, storage);
            }

            Map<String, Long> countSizePair = RunInfoUtils.getCountSizeRun(run);
            long count = countSizePair.get("count");
            long size = countSizePair.get("size");
            if (count <= 0 || size <= 0) {
                logger.log(Level.WARNING, "The " + run + " run was deleted");
                continue;
            }

            if (extension == null && count != lfns.size()) {
                logger.log(Level.WARNING, "The number of LFNs from rawdata_runs (" + count
                        + ") is different than the one in the LFNs list (" + lfns.size() + ")");
                //size = lfns.stream().mapToLong(lfn -> lfn.size).sum();
                continue;
            }

            Map<String, Object> values = new HashMap<>();
            values.put("run", run);
            values.put("counter", lfns.size());
            values.put("size", size);
            values.put("source", "Move Thread");
            values.put("targetse", targetSE);
            if (sourceSE != null) {
                values.put("sourcese", sourceSE);
                values.put("action", "move");
                values.put("log_message", "move to " + targetSE);
            } else {
                values.put("action", "copy");
                values.put("sourcese", storage);
                values.put("log_message", "copy to " + targetSE);
            }
            if (extension == null)
                values.put("filter", "all");
            else if (extension.equals(".tf"))
                values.put("filter", "tf");
            else if (extension.equals(".root"))
                values.put("filter", "ctf");
            else
                values.put("filter", extension);

            String insert = DBFunctions.composeInsert("rawdata_runs_action", values);
            logger.log(Level.INFO, insert);
            /*if (!db.query(insert)) {
                logger.log(Level.WARNING, "Insert in rawdata_runs_action failed for run: " + run + " " + db.getLastError());
            } else {
                TransferManager.addToTransfer(transferId, lfns);
                RunActionUtils.retrofitRawdataRunsLastAction(run);
                logger.log(Level.INFO, "The " + run + " run was successfully moved to " + targetSE);
            }*/
        }
    }
}
