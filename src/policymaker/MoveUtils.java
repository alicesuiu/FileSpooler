package policymaker;

import alien.catalogue.LFN;
import alien.config.ConfigUtils;
import alien.managers.TransferManager;
import alien.se.SEUtils;

import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MoveUtils {
    private static Logger logger = ConfigUtils.getLogger(MoveUtils.class.getCanonicalName());

    public static void moveRuns(Set<Long> runs, String targetSE, String sourceSE,
                                String logMessage, String extension, String storage, Integer limit) {
        int transferId = TransferManager.getTransferId(SEUtils.getSE(targetSE), logMessage, sourceSE);
        String action, sourcese, log_message;

        for (Long run : runs) {
            Set<LFN> lfns = RunInfoUtils.getLFNsFromRawdataDetails(run, extension);

            if (storage != null && !storage.isEmpty())
                RunInfoUtils.getLfnsFromCertainStorage(lfns, storage);

            if (limit != null && limit > 0 && limit <= 100) {
                lfns = RunInfoUtils.getFirstXLfns(lfns, limit);
                logger.log(Level.INFO, "Lfns list size after apply limit " + limit + "%: " + lfns.size());
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

             /*logger.log(Level.INFO, "Insert for run " + run + ": " + action + ", " + extension + ", " + log_message + ", "
                + lfns.size() + ", " + lfns.stream().mapToLong(lfn -> lfn.size).sum() + ", " + sourcese + ", " + targetSE);*/

            long ret = RunActionUtils.insertRunAction(run, action, extension, "Move Thread", log_message, lfns.size(),
                    lfns.stream().mapToLong(lfn -> lfn.size).sum(), sourcese, targetSE, "Done", limit, "Move Thread", null);

            if (ret > 0) {
                TransferManager.addToTransfer(transferId, lfns);
                RunActionUtils.retrofitRawdataRunsLastAction(run);
                logger.log(Level.INFO, "The " + run + " run was successfully " + action + " to " + targetSE);
            }
        }
    }
}
