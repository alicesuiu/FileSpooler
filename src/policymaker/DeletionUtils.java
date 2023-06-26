package policymaker;

import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.config.ConfigUtils;
import alien.se.SE;
import alien.se.SEUtils;
import lia.Monitor.Store.Fast.DB;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DeletionUtils {
    private static Logger logger = ConfigUtils.getLogger(DeletionUtils.class.getCanonicalName());
    private static final int RUN_DELETION_FAILED = -1;
    private static final SE ctaSE = SEUtils.getSE("ALICE::CERN::CTA");
    private static final SE eosSE = SEUtils.getSE("ALICE::CERN::EOS");
    private static final SE eosaliceo2SE = SEUtils.getSE("ALICE::CERN::EOSALICEO2");
    private static final SE kistiSE = SEUtils.getSE("ALICE::KISTI_GSDC::CDS");
    private static final SE ralSE = SEUtils.getSE("ALICE::RAL::CTA");
    private static final SE ndgfSE = SEUtils.getSE("ALICE::NDGF::DCACHE_TAPE");
    private static final SE fzkSE = SEUtils.getSE("ALICE::FZK::TAPE");
    private static final SE cnafSE = SEUtils.getSE("ALICE::CNAF::TAPE");
    private static final SE ccin2p3SE = SEUtils.getSE("ALICE::CCIN2P3::TAPE ");
    private static final SE ornlSE = SEUtils.getSE("ALICE::ORNL::PRF_EOS");
    private static int deleteRun(Long run, boolean logbookEntry, String extension, String storage) {
        DB db = new DB();
        String select = RunInfoUtils.getCollectionPathQuery(run, logbookEntry);
        String action = "delete", sourcese = "";
        SE se = null;

        db.query(select);
        while (db.moveNext()) {
            String collection_path = db.gets("collection_path");
            if (collection_path.isEmpty() || collection_path.isBlank())
                continue;
            logger.log(Level.INFO, collection_path);
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
                continue;
            }

            if (extension == null && count != lfns.size()) {
                logger.log(Level.WARNING, "The number of LFNs from rawdata_runs (" + count
                        + ") is different than the one in the LFNs list (" + lfns.size() + ")");
                //size = lfns.stream().mapToLong(lfn -> lfn.size).sum();
                continue;
            }

            if (storage != null && storage.length() > 0) {
                RunInfoUtils.getLfnsFromCertainStorage(lfns, storage);
                action = "delete replica";
                sourcese = storage;
                se = SEUtils.getSE(storage);
            }

            Iterator<LFN> lfnsIterator = lfns.iterator();
            boolean status = true;
            while (lfnsIterator.hasNext()) {
                LFN l = lfnsIterator.next();

                if (storage != null) {
                    GUID g = GUIDUtils.getGUID(l);
                    /*if (g.hasReplica(se)) {
                        if (g.removePFN(se, true) == null) {
                            status = false;
                            logger.log(Level.WARNING, "The deletion of the " + l.getName() + " failed");
                            continue;
                        }
                    } */
                } else {
                    /*if (!l.delete(true, false)) {
                        status = false;
                        logger.log(Level.WARNING, "The deletion of the " + l.getName() + " failed");
                        continue;
                    }*/
                }

                String update = "update rawdata set status='deleted'" +
                        " where lfn = '" + l.getCanonicalName() + "';";
                /*if (!db.syncUpdateQuery(update)) {
                    logger.log(Level.WARNING, "Status update in rawdata failed for run: " + run + " " + db.getLastError());
                }*/
            }

            select = "select daq_goodflag from rawdata_runs where run = " + run + ";";
            db.query(select);
            int iDaqGoodFlag = db.geti("daq_goodflag", -1);
            String sDaqGoodFlag = RunInfoUtils.getRunQuality(iDaqGoodFlag);
            if (sDaqGoodFlag == null)
                sDaqGoodFlag = "no logbook entry for this";
            String filter = "";
            if (extension == null)
                filter = "all";
            else if (extension.equals(".tf"))
                filter = "tf";
            else if (extension.equals(".root"))
                filter = "ctf";
            else
                filter = extension;

            if (status) {
                String insert = "insert into rawdata_runs_action (run, action, filter, counter, size, source, log_message, sourcese) " +
                        "values (" + run + ", " + action + ", " + filter + ", " + count + ", " + size + ", 'Deletion Thread', '" +
                        sDaqGoodFlag + " run', " + sourcese + ");";
                logger.log(Level.INFO, insert);
                /*if (!db.syncUpdateQuery(insert)) {
                    logger.log(Level.WARNING, "Insert in rawdata_runs_action failed for run: " + run + " " + db.getLastError());
                } else {
                    RunActionUtils.retrofitRawdataRunsLastAction(run);
                }*/
            } else {
                logger.log(Level.WARNING, "The deletion of the " + run + " run failed.");
                return RUN_DELETION_FAILED;
            }
        }
        return 0;
    }

    private static void deleteCheckStatus(Set<Long> runs, boolean logbookEntry, String extension, String storage) {
        List<Long> deletionFailed = new ArrayList<>();

        logger.log(Level.INFO, "List of runs that must be deleted: " + runs + ", nr: " + runs.size());
        for (Long run : runs) {
            int status = deleteRun(run, logbookEntry, extension, storage);
           if (status == RUN_DELETION_FAILED)
                deletionFailed.add(run);
            else
                logger.log(Level.INFO, "Successful deletion of the " + run + " run");
        }

        if (!deletionFailed.isEmpty())
            logger.log(Level.WARNING, "Runs with failed deletion: " + deletionFailed + ", nr: " + deletionFailed.size());
    }

    public static void deleteRunsNoLogbookEntry(Set<Long> runs, String extension, String storage) {
        deleteCheckStatus(runs, false, extension, storage);
    }

    public static void deleteRunsWithLogbookEntry(Set<Long> runs, String extension, String storage) {
        deleteCheckStatus(runs, true, extension, storage);
    }

    private static void deleteReplica(Long run, String collectionPath, String lfnPattern) {
        DB db = new DB();
        db.query("select partition from rawdata_runs where run = " + run + ";");
        String partition = db.gets("partition", "");

        Collection<LFN> lfns = LFNUtils.find(collectionPath + partition + "/" + run, lfnPattern, 0);
        Collection<UUID> uuids = new ArrayList<>(lfns.size());

        for (LFN l : lfns)
            uuids.add(l.guid);

        Set<GUID> guids = GUIDUtils.getGUIDs(uuids.toArray(new UUID[0]));
        int eosaliceo2Counter = 0;
        int eosCounter = 0;
        int ctaCounter = 0;
        int kistiCounter = 0;
        int ralCounter = 0;
        int ndgfCounter = 0;
        int fzkCounter = 0;
        int cnafCounter = 0;
        int ccin2p3Counter = 0;
        int ornlCounter = 0;

        for (GUID g : guids) {
            if (g.hasReplica(eosaliceo2SE)) {
                //g.removePFN(eosaliceo2SE, true);
                eosaliceo2Counter += 1;
            }

            if (g.hasReplica(ctaSE))
                ctaCounter += 1;

            if (g.hasReplica(eosSE))
                eosCounter += 1;

            if (g.hasReplica(kistiSE))
                kistiCounter += 1;

            if (g.hasReplica(ralSE))
                ralCounter += 1;

            if (g.hasReplica(ndgfSE))
                ndgfCounter += 1;

            if (g.hasReplica(fzkSE))
                fzkCounter += 1;

            if (g.hasReplica(cnafSE))
                cnafCounter += 1;

            if (g.hasReplica(ccin2p3SE))
                ccin2p3Counter += 1;

            if (g.hasReplica(ornlSE))
                ornlCounter += 1;
        }

        if (eosaliceo2Counter > 0) {
           /* if (ctaCounter == 0 && eosCounter == 0 && kistiCounter == 0 && ralCounter == 0 && ndgfCounter == 0 && fzkCounter == 0
                && cnafCounter == 0 && ccin2p3Counter == 0 && ornlCounter == 0) {

                counter += 1;*/
            logger.log(Level.INFO, "Run: " + run + " total files: " + guids.size() + ", EOSALICEO2: " + eosaliceo2Counter + ", CTA: " + ctaCounter
                    + ", EOS: " + eosCounter + ", KISTI: " + kistiCounter + ", RAL: " + ralCounter + ", NDGF: " + ndgfCounter
                    + ", FZK: " + fzkCounter + ", CNAF: " + cnafCounter + ", CCIN2P3: " + ccin2p3Counter + ", ORNL: " + ornlCounter);

            /* }*/

            /*db.query("insert into rawdata_runs_action (run, action, filter, counter, log_message, source, sourcese) values (" + run
                + ", 'delete replica', 'all', " + eosaliceo2Counter + ", 'Latchezar mail @ 2023-05-11 13:47', 'Alice.java', 'ALICE::CERN::EOSALICEO2');");
            RunActionUtils.retrofitRawdataRunsLastAction(run);*/
        }
    }

    public static void deleteReplicaRuns(Set<Long> runs) {
        for (Long run : runs)
            deleteReplica(run, "/alice/data/2022/", "/raw/*");
    }

    public static void deleteReplicaRuns(Long run, String collectionPath, String lfnPattern) {
        deleteReplica(run, collectionPath, lfnPattern);
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
