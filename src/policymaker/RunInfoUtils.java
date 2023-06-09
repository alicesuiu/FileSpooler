package policymaker;

import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.catalogue.GUIDUtils;
import alien.catalogue.GUID;
import alien.config.ConfigUtils;
import alien.managers.TransferManager;
import alien.user.JAKeyStore;
import alien.se.SE;
import alien.se.SEUtils;
import lazyj.Format;
import lia.Monitor.Store.Fast.DB;
import lazyj.DBFunctions;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.net.*;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RunInfoUtils {
    private static String URL_GET_RUN_INFO = "https://ali-bookkeeping.cern.ch/api/runs?filter[runNumbers]=";
    private static String URL_PATCH_RUN_INFO = "https://ali-bookkeeping.cern.ch/api/runs/?runNumber=";
    private static String URL_GET_CHANGES_RUN_INFO = "https://ali-bookkeeping.cern.ch/api/logs?filter[title]=has+changed";

    private static String LOGBOOOK_TOKEN_TEST = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6MCwidXNlcm5hbWUiOiJhbm9ueW1" +
            "vdXMiLCJuYW1lIjoiQW5vbnltb3VzIiwiYWNjZXNzIjoiYWRtaW4iLCJpYXQiOjE2Njg2ODI5NTAsImV4cCI6MTcwMDI0MDU1MCwiaX" +
            "NzIjoibzItdWkifQ.21BqUtJ1i13XIRldR0dyZLrq9jwzjfivWoEvL8c0Ot8";

    private static String LOGBOOOK_TOKEN_PROD = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6MCwidXNlcm5hbWUiOiJhbm9u" +
            "eW1vdXMiLCJuYW1lIjoiQW5vbnltb3VzIiwiYWNjZXNzIjoiYWRtaW4iLCJpYXQiOjE2NjU5OTQyMTgsImV4cCI6MTY5NzU1MTgxOCwia" +
            "XNzIjoibzItdWkifQ.zgrik9j1zn3NfyRlZrLBFHxDFdq5eiXH2IjJZFswl0M";
    private static Logger logger = ConfigUtils.getLogger(RunInfoUtils.class.getCanonicalName());

    private static String encode(final String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }

    private static String getRunInfoLogBookRequest(String runs) {
        String body = null;
        try {
            final KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509", "SunJSSE");
            kmf.init(JAKeyStore.getKeyStore(), JAKeyStore.pass);
            final SSLContext context = SSLContext.getInstance("SSL");
            context.init(kmf.getKeyManagers(), JAKeyStore.trusts, null);

            HttpClient httpClient = HttpClient.newBuilder()
                    .version(HttpClient.Version.HTTP_2)
                    .connectTimeout(Duration.ofSeconds(10))
                    .sslContext(context)
                    .build();

            HttpRequest request = HttpRequest.newBuilder()
                    .GET()
                    .uri(URI.create(URL_GET_RUN_INFO + encode(String.valueOf(runs))))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != HttpServletResponse.SC_OK) {
                logger.log(Level.WARNING, "Response code for GET req for run " + runs + " is " + response.statusCode());
                logger.log(Level.WARNING, "Response message for GET req for run " + runs + " is " + response.body());
                return null;
            }
            body = response.body();
        } catch (Exception e) {
            logger.log(Level.WARNING, "Communication error " + e);
        }
        return body;
    }

    private static Set<RunInfo> parseRunInfoJSON(String response) {
        Set<RunInfo> runInfoSet = new HashSet<>();
        JSONObject result = null;
        try {
            result = (JSONObject) new JSONParser().parse(response);
            if (result != null) {
                if (result.get("data") != null && result.get("data") instanceof JSONArray) {
                    JSONArray data = (JSONArray) result.get("data");
                    for (Object o : data) {
                        String runQuality = null, runType = null, detectors = null, lhcBeamMode = null,
                                aliceL3Polarity = null, aliceDipolePolarity = null, beamType = null, lhcPeriod = null,
                                ctfFileSize = null, tfFileSize = null, otherFileSize = null;
                        Long timeO2Start = null, timeO2End = null, runNumber = null, fillNumber = null,
                                startOfDataTransfer = null, endOfDataTransfer = null;
                        Double lhcBeamEnergy = null;
                        Integer ctfFileCount = null, tfFileCount = null, otherFileCount = null;
                        RunInfo runInfo = new RunInfo();

                        JSONObject block = (JSONObject) o;
                        if (block.get("runNumber") != null && block.get("runNumber") instanceof Number)
                            runNumber = ((Number) block.get("runNumber")).longValue();
                        runInfo.setRunNumber(runNumber);

                        if (block.get("runQuality") != null && block.get("runQuality") instanceof String)
                            runQuality = block.get("runQuality").toString();
                        runInfo.setRunQuality(runQuality);

                        if (block.get("detectors") != null && block.get("detectors") instanceof String)
                            detectors = block.get("detectors").toString();
                        runInfo.setDetectors(detectors);

                        if (block.get("startTime") != null && block.get("startTime") instanceof Number)
                            timeO2Start = ((Number) block.get("startTime")).longValue();
                        runInfo.setTimeO2Start(timeO2Start);

                        if (block.get("endTime") != null && block.get("endTime") instanceof Number)
                            timeO2End = ((Number) block.get("endTime")).longValue();
                        runInfo.setTimeO2End(timeO2End);

                        if (block.get("fillNumber") != null && block.get("fillNumber") instanceof Number)
                            fillNumber = ((Number) block.get("fillNumber")).longValue();
                        runInfo.setFillNumber(fillNumber);

                        if (block.get("lhcBeamEnergy") != null && block.get("lhcBeamEnergy") instanceof Number)
                            lhcBeamEnergy = ((Number) block.get("lhcBeamEnergy")).doubleValue();
                        runInfo.setLhcBeamEnergy(lhcBeamEnergy);

                        if (block.get("lhcBeamMode") != null && block.get("lhcBeamMode") instanceof String)
                            lhcBeamMode = block.get("lhcBeamMode").toString();
                        runInfo.setLhcBeamMode(lhcBeamMode);

                        if (block.get("aliceL3Polarity") != null && block.get("aliceL3Polarity") instanceof String)
                            aliceL3Polarity = block.get("aliceL3Polarity").toString();
                        runInfo.setAliceL3Polarity(aliceL3Polarity);

                        if (block.get("aliceDipolePolarity") != null && block.get("aliceDipolePolarity") instanceof String)
                            aliceDipolePolarity = block.get("aliceDipolePolarity").toString();
                        runInfo.setAliceDipolePolarity(aliceDipolePolarity);

                        if (block.get("definition") != null && block.get("definition") instanceof String)
                            runType = block.get("definition").toString();
                        runInfo.setRunType(runType);

                        if (block.get("lhcFill") != null && block.get("lhcFill") instanceof JSONObject) {
                            JSONObject lhcFill = (JSONObject) block.get("lhcFill");
                            if (lhcFill.get("beamType") != null && lhcFill.get("beamType") instanceof String)
                                beamType = lhcFill.get("beamType").toString();
                        }
                        runInfo.setBeamType(beamType);

                        if (block.get("lhcPeriod") != null && block.get("lhcPeriod") instanceof String)
                            lhcPeriod = block.get("lhcPeriod").toString();
                        runInfo.setLhcPeriod(lhcPeriod);

                        if (block.get("startOfDataTransfer") != null && block.get("startOfDataTransfer") instanceof Number)
                            startOfDataTransfer = ((Number) block.get("startOfDataTransfer")).longValue();
                        runInfo.setStartOfDataTransfer(startOfDataTransfer);

                        if (block.get("endOfDataTransfer") != null && block.get("endOfDataTransfer") instanceof Number)
                            endOfDataTransfer = ((Number) block.get("endOfDataTransfer")).longValue();
                        runInfo.setEndOfDataTransfer(endOfDataTransfer);

                        if (block.get("ctfFileSize") != null && block.get("ctfFileSize") instanceof String)
                            ctfFileSize = block.get("ctfFileSize").toString();
                        runInfo.setCtfFileSize(ctfFileSize);

                        if (block.get("tfFileSize") != null && block.get("tfFileSize") instanceof String)
                            tfFileSize = block.get("tfFileSize").toString();
                        runInfo.setTfFileSize(tfFileSize);

                        if (block.get("otherFileSize") != null && block.get("otherFileSize") instanceof String)
                            otherFileSize = block.get("otherFileSize").toString();
                        runInfo.setOtherFileSize(otherFileSize);

                        if (block.get("ctfFileCount") != null && block.get("ctfFileCount") instanceof Number)
                            ctfFileCount = ((Number) block.get("ctfFileCount")).intValue();
                        runInfo.setCtfFileCount(ctfFileCount);

                        if (block.get("tfFileCount") != null && block.get("tfFileCount") instanceof Number)
                            tfFileCount = ((Number) block.get("tfFileCount")).intValue();
                        runInfo.setTfFileCount(tfFileCount);

                        if (block.get("otherFileCount") != null && block.get("otherFileCount") instanceof Number)
                            otherFileCount = ((Number) block.get("otherFileCount")).intValue();
                        runInfo.setOtherFileCount(otherFileCount);

                        runInfoSet.add(runInfo);
                    }
                }
            }
        } catch (ParseException ex) {
            logger.log(Level.WARNING, "Caught error while parsing the JSON response " + response + " " + ex);
        }
        return runInfoSet;
    }

    private static Set<RunInfo> parseRunInfoLogsJSON(String response) {
        Set<RunInfo> runInfoSet = new HashSet<>();
        JSONObject result = null;
        try {
            result = (JSONObject) new JSONParser().parse(response);
            if (result != null) {
                if (result.get("data") != null && result.get("data") instanceof JSONArray) {
                    JSONArray data = (JSONArray) result.get("data");
                    for (Object o : data) {
                        Long createdAt = null, runNumber = null;
                        JSONObject block = (JSONObject) o;
                        if (block.get("createdAt") != null && block.get("createdAt") instanceof Number) {
                            createdAt = ((Number) block.get("createdAt")).longValue();
                            if (block.get("runs") != null && block.get("runs") instanceof JSONArray) {
                                JSONArray runs = (JSONArray) block.get("runs");
                                for (Object or : runs) {
                                    RunInfo runInfo = new RunInfo();
                                    JSONObject runBlock = (JSONObject) or;
                                    if (runBlock.get("runNumber") != null && runBlock.get("runNumber") instanceof Number) {
                                        runNumber = ((Number) runBlock.get("runNumber")).longValue();
                                        runInfo.setRunNumber(runNumber);
                                        runInfo.setLastModified(createdAt);
                                        runInfoSet.add(runInfo);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (ParseException ex) {
            logger.log(Level.WARNING, "Caught error while parsing the JSON response " + response + " " + ex);
            return null;
        }

        return runInfoSet;
    }

    public static Set<RunInfo> getRunInfoFromLogBook(String runs) {
        Set<RunInfo> runInfoSet = new HashSet<>();
        String response = getRunInfoLogBookRequest(runs);
        if (response != null)
            runInfoSet = parseRunInfoJSON(response);
        return runInfoSet;
    }

    public static int sendRunInfoToLogBook(Long run, Map<String, Object> fields) {
        try {
            if (fields.isEmpty())
                return -1;

            String json = Format.toJSON(fields).toString();

            final KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509", "SunJSSE");
            kmf.init(JAKeyStore.getKeyStore(), JAKeyStore.pass);
            final SSLContext context = SSLContext.getInstance("SSL");
            context.init(kmf.getKeyManagers(), JAKeyStore.trusts, null);

            HttpClient httpClient = HttpClient.newBuilder()
                    .version(HttpClient.Version.HTTP_2)
                    .sslContext(context)
                    .build();

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(URL_PATCH_RUN_INFO + encode(String.valueOf(run))))
                    .method("PATCH", HttpRequest.BodyPublishers.ofString(json))
                    .header("Content-Type", "application/json")
                    .build();

            HttpResponse<String> response =
                    httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != HttpServletResponse.SC_OK) {
                logger.log(Level.WARNING, "Response code for PATCH req for run " + run + " is " + response.statusCode());
                logger.log(Level.WARNING, "Response message for PATCH req for run " + run + " is " + response.body());
                logger.log(Level.INFO, json);
            }
            return response.statusCode();
        } catch (IOException | InterruptedException | UnrecoverableKeyException | KeyManagementException | NoSuchProviderException |
                 NoSuchAlgorithmException | KeyStoreException e) {
            logger.log(Level.WARNING,"Communication error " + e);
        }
        return -1;
    }

    public static void fetchRunInfo(Set<Long> runs) throws HandleException {
        List<Long> missingTimeO2End = new ArrayList<>();
        List<Long> missingLogbookRecord = new ArrayList<>();
        String msg;
        for (Long run : runs) {
            Map<String, Object> fields = getRunParamsForLogBook(String.valueOf(run));
            int status = sendRunInfoToLogBook(run, fields);
            Set<RunInfo> runInfos;
            if (status == HttpServletResponse.SC_OK) {
                runInfos = RunInfoUtils.getRunInfoFromLogBook(String.valueOf(run));
                if (!runInfos.isEmpty()) {
                    RunInfo runInfo = runInfos.iterator().next();
                    if (runInfo.getRunNumber() == null) {
                        missingLogbookRecord.add(run);
                        continue;
                    }
                    
                    if (runInfo.getTimeO2End() != null && runInfo.getTimeO2End() > 0) {
                        if (runInfo.getTimeO2Start() != null && runInfo.getTimeO2Start() > 0)
                            runInfo.setRunDuration(runInfo.getTimeO2End() - runInfo.getTimeO2Start());
                        logger.log(Level.INFO, runInfo.toString());
                        runInfo.processQuery();
                    } else {
                        missingTimeO2End.add(run);
                    }
                } else {
                    missingLogbookRecord.add(run);
                }
            } else {
                throw new HandleException("The PATCH request to the logbook did not work. We caught a HTTP error code.", status);
            }
        }

        if (!missingTimeO2End.isEmpty()) {
            msg = "Runs with missing timestamp information: " + missingTimeO2End + ", nr: " + missingTimeO2End.size();
            throw new HandleException(msg, missingTimeO2End);
        }
        if (!missingLogbookRecord.isEmpty()) {
            msg = "Runs with missing logbook records: " + missingLogbookRecord + ", nr: " + missingLogbookRecord.size();
            throw new HandleException(msg, missingLogbookRecord);
        }
    }

    private static Map<String, Object> getRunParamsForLogBook(String run) {
        Map<String, Object> fields = new HashMap<>();
        long startOfDataTransfer, endOfDataTransfer, ctfFileSize, tfFileSize, otherFileSize;
        int ctfFileCount, tfFileCount, otherFileCount;
        DB db = new DB();

        String select = "select mintime, maxtime, tf_file_count, tf_file_size, ctf_file_count, ctf_file_size, " +
                "other_file_count, other_file_size from rawdata_runs where run=" + run + ";";
        db.query(select);
        if (!db.moveNext())
            return fields;

        startOfDataTransfer = db.getl(1) * 1000;
        endOfDataTransfer = db.getl(2) * 1000;
        tfFileCount = db.geti(3);
        tfFileSize = db.getl(4);
        ctfFileCount = db.geti(5);
        ctfFileSize = db.getl(6);
        otherFileCount = db.geti(7);
        otherFileSize = db.getl(8);

        fields.put("startOfDataTransfer", startOfDataTransfer);
        fields.put("endOfDataTransfer", endOfDataTransfer);
        fields.put("ctfFileCount", ctfFileCount);
        fields.put("ctfFileSize", String.valueOf(ctfFileSize));
        fields.put("tfFileCount", tfFileCount);
        fields.put("tfFileSize", String.valueOf(tfFileSize));
        fields.put("otherFileCount", otherFileCount);
        fields.put("otherFileSize", String.valueOf(otherFileSize));
        return fields;
    }

    public static Set<Long> getSetOfRunsFromCertainSelect(String select) {
        Map<Long, MonitorRun> activeRunsMap = MonitorRunUtils.getActiveRuns();
        Set<Long> activeRunsSet = new HashSet<>();
        for (Map.Entry<Long, MonitorRun> entry : activeRunsMap.entrySet()) {
            if (entry.getValue().getCnt() != 0) {
                activeRunsSet.add(entry.getKey());
            }
        }

        Set<Long> dbRunsList = new HashSet<>();
        DB db = new DB();
        db.query(select);
        while (db.moveNext()) {
            long run = db.geti("run");
            dbRunsList.add(run);
        }
        dbRunsList.removeAll(activeRunsSet);
        return dbRunsList;
    }

    public static void retrofitCountersInRawdataRuns(long mintime, long maxtime) {
        Long ctfFileSize = null, tfFileSize = null, otherFileSize = null;
        Integer ctfFileCount = null, tfFileCount = null, otherFileCount = null;

        String select = "select rr.run from rawdata_runs rr left outer join rawdata_runs_action ra on " +
                "ra.run=rr.run and action='delete' where action is null and mintime >= " + mintime +
                " and maxtime <= " + maxtime + ";";
        Set<Long> dbRunsList = getSetOfRunsFromCertainSelect(select);
        DB db = new DB();

        for (Long run : dbRunsList) {
            String selectCTF = "select count(size), sum(size) from rawdata_details where run=" + run + " and lfn like '%/o2_ctf_%.root';";
            db.query(selectCTF);
            if (db.moveNext()) {
                ctfFileCount = db.geti(1);
                ctfFileSize = db.getl(2);
            }

            String selectTF = "select count(size), sum(size) from rawdata_details where run=" + run + " and lfn like '%/o2_rawtf_%.tf';";
            db.query(selectTF);
            if (db.moveNext()) {
                tfFileCount = db.geti(1);
                tfFileSize = db.getl(2);
            }

            String selectOther = "select count(size), sum(size) from rawdata_details where run=" + run
                    + " and lfn not like '%/o2_ctf_%.root' and lfn not like '%/o2_rawtf_%.tf';";
            db.query(selectOther);
            if (db.moveNext()) {
                otherFileCount = db.geti(1);
                otherFileSize = db.getl(2);
            }

            if (ctfFileCount != null && tfFileCount != null && otherFileCount != null &&
                    ctfFileSize != null && tfFileSize != null && otherFileSize != null) {
                String update = "UPDATE rawdata_runs SET tf_file_count=" + tfFileCount + ", tf_file_size=" + tfFileSize
                        + ", ctf_file_count=" + ctfFileCount + ", ctf_file_size=" + ctfFileSize + ", other_file_count="
                        + otherFileCount + ", other_file_size=" + otherFileSize + " WHERE run=" + run + ";";

                if (!db.syncUpdateQuery(update))
                    logger.log(Level.WARNING, "Update in rawdata_runs for run " + run + " failed.");
            }
        }
    }

    public static Set<RunInfo> getRunInfoChangesFromLogBook(long mintime, long maxtime) {
        Set<RunInfo> runInfoSet = new HashSet<>();
        try {
            final KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509", "SunJSSE");
            kmf.init(JAKeyStore.getKeyStore(), JAKeyStore.pass);
            final SSLContext context = SSLContext.getInstance("SSL");
            context.init(kmf.getKeyManagers(), JAKeyStore.trusts, null);

            String url = URL_GET_CHANGES_RUN_INFO + "&filter[created][from]=" + encode(String.valueOf(mintime))
                    + "&filter[created][to]=" + encode(String.valueOf(maxtime)) + "&token=" + LOGBOOOK_TOKEN_PROD;

            HttpClient httpClient = HttpClient.newBuilder()
                    .version(HttpClient.Version.HTTP_2)
                    .connectTimeout(Duration.ofSeconds(10))
                    .sslContext(context)
                    .build();

            HttpRequest request = HttpRequest.newBuilder()
                    .GET()
                    .uri(URI.create(url))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != HttpServletResponse.SC_OK) {
                logger.log(Level.WARNING, "Response code for GET changes req for mintime " + mintime + " and maxtime "
                        + maxtime + ": " + response.statusCode());
                logger.log(Level.WARNING, "Response message for GET changes req for mintime " + mintime + " and maxtime "
                        + maxtime + ": " + response.body());
                return null;
            }

            runInfoSet = parseRunInfoLogsJSON(response.body());
        } catch (Exception e) {
            logger.log(Level.WARNING, "Communication error " + e);
        }

        return runInfoSet;
    }

    public static Set<Long> getLastUpdatedRuns(long mintime, long maxtime) throws HandleException {
        Set<RunInfo> runInfoSet = RunInfoUtils.getRunInfoChangesFromLogBook(mintime, maxtime);
        Set<Long> runs = new HashSet<>();
        if (runInfoSet != null) {
            DB db = new DB();
            for (RunInfo ri : runInfoSet) {
                String select = "select rr.run, rr.lastmodified from rawdata_runs rr left outer join rawdata_runs_action " +
                        "ra on ra.run=rr.run and action='delete' where action is null and rr.run=" + ri.getRunNumber() + ";";
                db.query(select);
                if (db.moveNext()) {
                    long lastModifiedDB = db.geti("lastmodified") * 1000L;
                    if (ri.getLastModified() > lastModifiedDB && lastModifiedDB > 0)
                        runs.add(ri.getRunNumber());
                }
            }
        } else {
            throw new HandleException("Get last updated runs between: [" + mintime + ", " + maxtime + "] failed.");
        }
        return runs;
    }

    public static int getDaqGoodFlag(String runQuality) {
        if (runQuality == null)
            return -1;
        if (runQuality.equalsIgnoreCase("bad"))
            return 0;
        if (runQuality.equalsIgnoreCase("good"))
            return 1;
        if (runQuality.equalsIgnoreCase("test"))
            return 2;
        return -1;
    }

    public static String getRunQuality(int daqGoodFlag) {
        if (daqGoodFlag == 0)
            return "bad";
        if (daqGoodFlag == 1)
            return "good";
        if (daqGoodFlag == 2)
            return "test";
        return null;
    }

    public static String getRunQualityFromDB(Long run) {
        String select = "select daq_goodflag from rawdata_runs where run = " + run + ";";
        DB db = new DB(select);
        int iDaqGoodFlag = db.geti("daq_goodflag", -1);
        return getRunQuality(iDaqGoodFlag);
    }

    public static String getRunQualityFromLogbook(Long run) {
        String runQuality = null;
        Set<RunInfo> runInfos = RunInfoUtils.getRunInfoFromLogBook(String.valueOf(run));
        if (!runInfos.isEmpty()) {
            RunInfo runInfo = runInfos.iterator().next();
            runQuality = runInfo.getRunQuality();
        }
        return runQuality;
    }

    public static void deleteRunsWithCertainRunQuality(String runQuality) {
        int daqGoodFlag = getDaqGoodFlag(runQuality);
        if (Arrays.asList(0, 1, 2).contains(daqGoodFlag)) {
            long lastmodified = ZonedDateTime.now(ZoneId.of("Europe/Zurich"))
                    .minusWeeks(4).toInstant().toEpochMilli() / 1000;
            long mintime = 1633039200; // 1.10.2021
            long maxtime = 1669158000; // 23.11.2022
            String select = "select rr.run from rawdata_runs rr left outer join rawdata_runs_action ra on " +
                    "ra.run=rr.run and action='delete' where mintime >= " + mintime + " and daq_goodflag = " +
                    daqGoodFlag + " and action is null and maxtime <= " + maxtime + ";"; // and lastmodified <=" + lastmodified + ";";
            Set<Long> runs = RunInfoUtils.getSetOfRunsFromCertainSelect(select);
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

            /*if (!updatedRuns.isEmpty()) {
                logger.log(Level.WARNING, "Runs with different info in Logbook than database: " + updatedRuns + ", nr: " + updatedRuns.size());
                RunInfoUtils.fetchRunInfo(updatedRuns);
            }*/
        } else {
            logger.log(Level.WARNING, "The received run quality " + runQuality + " is invalid.");
        }
    }

    //private static final int RUN_ALREADY_DELETED = -1;
    private static final int RUN_CONTAINS_OTHER_FILES = -1;
    private static final int RUN_QUERY_FAILED = -2;
    private static final int RUN_DELETION_FAILED = -3;

    private static String getCollectionPathQuery(Long run, boolean logbookEntry) {
        String prefix = "";
        if (logbookEntry) {
            Set<RunInfo> runInfos = RunInfoUtils.getRunInfoFromLogBook(String.valueOf(run));
            if (!runInfos.isEmpty()) {
                RunInfo runInfo = runInfos.iterator().next();
                if (runInfo.getTimeO2Start() != null && runInfo.getTimeO2Start() > 0)
                    prefix = String.valueOf(new Date(runInfo.getTimeO2Start()).getYear() + 1900);
            }
        }
        String select = "select collection_path from rawdata_runs where run = " + run +
                " and collection_path like '%/alice/data/" + prefix + "%/collection';";
        return select;
    }

    private static Map<String, Long> getCountSizeRun(Long run) {
        DB db = new DB();
        Map<String, Long> countSizeMap = new HashMap<>();
        String select = "select count(1), sum(size) as size from rawdata_details where run = " + run + ";";
        db.query(select);
        if (!db.moveNext()) {
            logger.log(Level.WARNING, select + " failed");
            countSizeMap.put("count", 0L);
            countSizeMap.put("size", 0L);
        }
        countSizeMap.put("count", db.getl("count"));
        countSizeMap.put("size", db.getl("size"));
        return countSizeMap;
    }

    private static Set<LFN> getLFNsFromCollection(String collectionPath) {
        Set<LFN> lfns = new HashSet<>(LFNUtils
                .find(collectionPath.replaceAll("collection", ""), "*", 0));
        logger.log(Level.INFO, "Lfns list size: " + lfns.size());
        if (lfns.size() == 0)
            return lfns;

        Iterator<LFN> lfnsIterator = lfns.iterator();
        while (lfnsIterator.hasNext()) {
            LFN lfn = lfnsIterator.next();
            if (!lfn.isFile()) {
                logger.log(Level.WARNING, "This " + lfn.getCanonicalName() + " lfn is not a file.");
                lfnsIterator.remove();
            }
        }

        return lfns;
    }

    private static int deleteRun(Long run, boolean logbookEntry) {
        DB db = new DB();

        String prefix = "";
        if (logbookEntry) {
            Set<RunInfo> runInfos = RunInfoUtils.getRunInfoFromLogBook(String.valueOf(run));
            if (!runInfos.isEmpty()) {
                RunInfo runInfo = runInfos.iterator().next();
                if (runInfo.getTimeO2Start() != null && runInfo.getTimeO2Start() > 0)
                    prefix = String.valueOf(new Date(runInfo.getTimeO2Start()).getYear() + 1900);
            }
        }
        String select = "select collection_path from rawdata_runs where run = " + run +
                " and collection_path like '%/alice/data/" + prefix + "%/collection';";
        db.query(select);
        while (db.moveNext()) {
            String collection_path = db.gets("collection_path");
            if (collection_path.isEmpty() || collection_path.isBlank())
                continue;
            logger.log(Level.INFO, collection_path);
            Set<LFN> lfns = new HashSet<>(LFNUtils
                    .find(collection_path.replaceAll("collection", ""), "*", 0));
            logger.log(Level.INFO, "Lfns list size: " + lfns.size());
            if (lfns.size() == 0) {
                logger.log(Level.INFO, "The " + run + " run with collection path " + collection_path + " was deleted");
                continue;
            }

            Iterator<LFN> lfnsIterator = lfns.iterator();
            while (lfnsIterator.hasNext()) {
                LFN lfn = lfnsIterator.next();
                if (!lfn.isFile()) {
                    logger.log(Level.WARNING, "This " + lfn.getCanonicalName() + " lfn is not a file.");
                    lfnsIterator.remove();
                    continue;
                }
                String lfnName = lfn.getCanonicalName().substring(lfn.getCanonicalName().lastIndexOf('/') + 1);
                if (!lfnName.endsWith(".root") && !lfnName.endsWith(".tf")) {
                    logger.log(Level.WARNING, lfn.getCanonicalName() + " has incorrect extension");
                    return RUN_CONTAINS_OTHER_FILES;
                }
            }

            select = "select count(1), sum(size) as size from rawdata_details where run = " + run + ";";
            db.query(select);
            if (!db.moveNext()) {
                logger.log(Level.WARNING, select + " failed");
                return RUN_QUERY_FAILED;
            }
            long count = db.getl("count");
            long size = db.getl("size");
            lfnsIterator = lfns.iterator();
            boolean status = true;
            while (lfnsIterator.hasNext()) {
                LFN l = lfnsIterator.next();
                /*if (!l.delete(true, false)) {
                    status = false;
                    logger.log(Level.WARNING, "The deletion of the " + l.getName() + " failed");
                    continue;
                }*/

                String update = "update rawdata set status='deleted'" +
                        " where lfn = '" + l.getCanonicalName() + "';";
                /*if (!db.syncUpdateQuery(update)) {
                    logger.log(Level.WARNING, "Status update in rawdata failed for run: " + run + " " + db.getLastError());
                }*/
            }

            select = "select daq_goodflag from rawdata_runs where run = " + run + ";";
            db.query(select);
            int iDaqGoodFlag = db.geti("daq_goodflag", -1);
            String sDaqGoodFlag = getRunQuality(iDaqGoodFlag);
            if (sDaqGoodFlag == null)
                sDaqGoodFlag = "no logbook entry for this";

            if (status) {
                String insert = "insert into rawdata_runs_action (run, action, filter, counter, size, source, log_message) " +
                        "values (" + run + ", 'delete', 'all', " + count + ", " + size + ", 'Deletion Thread', '" +
                        sDaqGoodFlag + " run')";
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


    private static void deleteCheckStatus(Set<Long> runs, boolean logbookEntry) {
        List<Long> containsOtherFiles = new ArrayList<>();
        List<Long> queryFailed = new ArrayList<>();
        List<Long> deletionFailed = new ArrayList<>();

        logger.log(Level.INFO, "List of runs that must be deleted: " + runs + ", nr: " + runs.size());
        for (Long run : runs) {
            int status = deleteRun(run, logbookEntry);
            if (status == RUN_CONTAINS_OTHER_FILES)
                containsOtherFiles.add(run);
            else if (status == RUN_QUERY_FAILED)
                queryFailed.add(run);
            else if (status == RUN_DELETION_FAILED)
                deletionFailed.add(run);
            else
                logger.log(Level.INFO, "Successful deletion of the " + run + " run");
        }

        if (!containsOtherFiles.isEmpty())
            logger.log(Level.WARNING, "Runs with files other than TF and CTF: " + containsOtherFiles + ", nr: " + containsOtherFiles.size());
        if (!queryFailed.isEmpty())
            logger.log(Level.WARNING, "Runs with failed query: " + queryFailed + ", nr: " + queryFailed.size());
        if (!deletionFailed.isEmpty())
            logger.log(Level.WARNING, "Runs with failed deletion: " + deletionFailed + ", nr: " + deletionFailed.size());
    }

    public static void deleteRunsNoLogbookEntry(Set<Long> runs) {
        deleteCheckStatus(runs, false);
    }

    public static void deleteRunsWithLogbookEntry(Set<Long> runs) {
        deleteCheckStatus(runs, true);
    }


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

    private static void deleteReplica(Long run, String collectionPath, String lfnPattern) {
        Collection<LFN> lfns = LFNUtils.find(collectionPath, lfnPattern, 0);
        Collection<UUID> uuids = new ArrayList<>(lfns.size());

        for (LFN l : lfns)
            uuids.add(l.guid);

        //logger.log(Level.INFO, "Requesting GUIDs for " + uuids.size() + " objects");

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
        /*int notOnCTA = 0;
        int notOnEOS = 0;*/

        for (GUID g : guids) {
            if (g.hasReplica(eosaliceo2SE)) {
                //g.removePFN(eosaliceo2SE, true);
                eosaliceo2Counter += 1;
            }

            /*if (g.hasReplica(ctaSE))
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
                ornlCounter += 1;*/

            /*if (g.hasReplica(ctaSE) && g.hasReplica(eosSE)) {
                g.removePFN(eosSE, true);
                allGood++;
            }
            else if (!g.hasReplica(ctaSE))
                notOnCTA++;
            else
                notOnEOS++;*/
        }

        logger.log(Level.INFO, "Run: " + run + " total files: " + guids.size() + ", EOSALICEO2: " + eosaliceo2Counter + ", CTA: " + ctaCounter
                + ", EOS: " + eosCounter + ", KISTI: " + kistiCounter + ", RAL: " + ralCounter + ", NDGF: " + ndgfCounter
                + ", FZK: " + fzkCounter + ", CNAF: " + cnafCounter + ", CCIN2P3: " + ccin2p3Counter + ", ORNL: " + ornlCounter);

        /*DB db = new DB();
        db.query("insert into rawdata_runs_action (run, action, filter, counter, log_message, source, sourcese) values (" + run
            + ", 'delete replica', 'tf', " + eosaliceo2Counter + ", 'Latchezar mail @ 2023-05-11 13:47', 'Alice.java', 'ALICE::CERN::EOSALICEO2');");
        RunActionUtils.retrofitRawdataRunsLastAction(run);*/
    }

    public static void deleteReplicaRuns(Set<Long> runs) {
        for (Long run : runs)
            deleteReplica(run, "/alice/data/2023/LHC23l/" + run, "/raw/*.root");
    }

    public static void deleteReplicaRuns(Long run, String collectionPath, String lfnPattern) {
        deleteReplica(run, collectionPath, lfnPattern);
    }

    private static Set<LFN> moveRun(Long run, boolean logbookEntry) {
        DB db = new DB();
        String select = getCollectionPathQuery(run, logbookEntry);
        Set<LFN> lfnsToTransfer = new HashSet<>();
        db.query(select);
        while (db.moveNext()) {
            String collectionPath = db.gets("collection_path");
            if (collectionPath.isEmpty() || collectionPath.isBlank())
                continue;
            logger.log(Level.INFO, collectionPath);
            Set<LFN> lfns = getLFNsFromCollection(collectionPath);
            if (lfns.size() == 0) {
                logger.log(Level.WARNING, "The " + run + " run with collection path " + collectionPath + " was deleted");
                continue;
            }
            lfnsToTransfer.addAll(lfns);
        }
        return lfnsToTransfer;
    }

    public static void moveRuns(Set<Long> runs, String targetSE, String sourceSE, boolean logbookEntry, String logMessage) {
        int transferId = TransferManager.getTransferId(SEUtils.getSE(targetSE), logMessage, sourceSE);
        DB db = new DB();

        for (Long run : runs) {
            Set<LFN> lfns = moveRun(run, logbookEntry);
            boolean ok = true;
            for (LFN lfn : lfns) {
                String lfnName = lfn.getCanonicalName().substring(lfn.getCanonicalName().lastIndexOf('/') + 1);
                if (!lfnName.endsWith(".root") && !lfnName.endsWith(".tf")) {
                    logger.log(Level.WARNING, lfn.getCanonicalName() + " has incorrect extension");
                    ok = false;
                    break;
                }
            }

            if (!ok) {
                continue;
            }

            Map<String, Long> countSizePair = getCountSizeRun(run);
            long count = countSizePair.get("count");
            long size = countSizePair.get("size");
            if (count <= 0 || size <= 0) {
                logger.log(Level.WARNING, "The " + run + " run was deleted");
                continue;
            }

            if (count != lfns.size()) {
                logger.log(Level.WARNING, "The number of LFNs from rawdata_runs (" + count
                        + ") is different than the one in the LFNs list (" + lfns.size() + ")");
                continue;
            }

            Map<String, Object> values = new HashMap<>();
            values.put("run", run);
            values.put("action", "move");
            values.put("filter", "all");
            values.put("counter", count);
            values.put("size", size);
            values.put("source", "Move Thread");
            values.put("log_message", "move to " + targetSE);
            values.put("targetse", targetSE);
            if (sourceSE != null)
                values.put("sourcese", sourceSE);
            String insert = DBFunctions.composeInsert("rawdata_runs_action", values);
            logger.log(Level.INFO, insert);
            /*if (!db.query(insert)) {
                logger.log(Level.WARNING, "Insert in rawdata_runs_action failed for run: " + run + " " + db.getLastError());
            } else {
                RunActionUtils.retrofitRawdataRunsLastAction(run);
                TransferManager.addToTransfer(transferId, lfns);
                logger.log(Level.INFO, "The " + run + " run was successfully moved to " + targetSE);
            }*/
        }
    }
}
