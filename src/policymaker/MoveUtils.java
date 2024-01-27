package policymaker;

import alien.catalogue.LFN;
import alien.config.ConfigUtils;
import alien.managers.TransferManager;
import alien.se.SEUtils;

import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MoveUtils {
    private static Logger logger = ConfigUtils.getLogger(MoveUtils.class.getCanonicalName());

    public static void moveRuns(Set<Long> runs, String targetSE, String sourceSE, Boolean logbookEntry,
                                String logMessage, String extension, String storage, Integer limit) {
        int transferId = TransferManager.getTransferId(SEUtils.getSE(targetSE), logMessage, sourceSE);
        String action, sourcese, log_message, filter;

        for (Long run : runs) {
            Set<LFN> lfns;

            if (logbookEntry == null)
                lfns = RunInfoUtils.getLFNsFromRawdataDetails(run);
            else
                lfns = RunInfoUtils.getLFNsFromCollection(run, logbookEntry);

            if (extension != null && extension.length() > 0)
                RunInfoUtils.getLfnsWithCertainExtension(lfns, extension);
            else
                RunInfoUtils.getAllLFNs(lfns);

            if (storage != null && storage.length() > 0)
                RunInfoUtils.getLfnsFromCertainStorage(lfns, storage);

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
            }

            if (limit != null && limit > 0) {
                lfns = RunInfoUtils.getFirstXLfns(lfns, limit);
                //logger.log(Level.INFO, "lfns: " + lfns);
            }

            if (sourceSE != null) {
                sourcese = sourceSE;
                action = "move";
                log_message = "move to " + targetSE;
            } else {
                sourcese = storage;
                action = "copy";
                log_message =  "copy to " + targetSE;
            }

            if (extension == null)
                filter = "all";
            else if (extension.equals(".tf"))
                filter = "tf";
            else if (extension.equals(".root"))
                filter = "ctf";
            else
                filter = extension;

             /*logger.log(Level.INFO, "Insert for run " + run + ": " + action + ", " + filter + ", " + log_message + ", "
                + lfns.size() + ", " + lfns.stream().mapToLong(lfn -> lfn.size).sum() + ", " + sourcese + ", " + targetSE);*/

            int ret = RunActionUtils.insertRunAction(run, action, filter, "Move Thread", log_message, lfns.size(),
                    lfns.stream().mapToLong(lfn -> lfn.size).sum(), sourcese, targetSE, "Done", limit);

            if (ret >= 0) {
                TransferManager.addToTransfer(transferId, lfns);
                RunActionUtils.retrofitRawdataRunsLastAction(run);
                logger.log(Level.INFO, "The " + run + " run was successfully " + action + " to " + targetSE);
            }
        }
    }
}
