package policymaker;

import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.catalogue.LFNUtils;
import alien.catalogue.GUIDUtils;
import alien.catalogue.GUID;
import alien.config.ConfigUtils;
import alien.user.JAKeyStore;
import alien.se.SE;
import alien.se.SEUtils;
import lazyj.Format;
import lazyj.mail.Mail;
import lazyj.mail.Sendmail;
import lazyj.DBFunctions;
import lia.Monitor.Store.Fast.DB;
import lia.Monitor.monitor.AppConfig;
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
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class RunInfoUtils {
    private static String URL_GET_RUN_INFO = "https://ali-bookkeeping.cern.ch/api/runs?filter[runNumbers]=";
    private static String URL_PATCH_RUN_INFO = "https://ali-bookkeeping.cern.ch/api/runs/?runNumber=";
    private static String URL_GET_CHANGES_RUN_INFO = "https://ali-bookkeeping.cern.ch/api/logs?filter[title]=has+changed";

    private static String LOGBOOOK_TOKEN_TEST = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6MCwidXNlcm5hbWUiOiJhbm9ueW1" +
            "vdXMiLCJuYW1lIjoiQW5vbnltb3VzIiwiYWNjZXNzIjoiYWRtaW4iLCJpYXQiOjE2Njg2ODI5NTAsImV4cCI6MTcwMDI0MDU1MCwiaX" +
            "NzIjoibzItdWkifQ.21BqUtJ1i13XIRldR0dyZLrq9jwzjfivWoEvL8c0Ot8";

    private static String LOGBOOOK_TOKEN_PROD = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6MCwidXNlcm5hbWUiOiJhbm9ue" +
            "W1vdXMiLCJuYW1lIjoiQW5vbnltb3VzIiwiYWNjZXNzIjoiYWRtaW4iLCJpYXQiOjE2OTQxNTk2MTgsImV4cCI6MTcyNTcxNzIxOCwiaXN" +
            "zIjoibzItdWkifQ.jV2TAreh9xYwpznvoJ5Bge-jHtVxBigUVIT1CtkL_54";
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
                    .uri(URI.create(URL_GET_RUN_INFO + encode(String.valueOf(runs)) + "&token=" + LOGBOOOK_TOKEN_PROD))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != HttpServletResponse.SC_OK) {
                logger.log(Level.WARNING, "[Bookkeeping] Response code for GET req for run " + runs + " is " + response.statusCode());
                logger.log(Level.WARNING, "[Bookkeeping] Response message for GET req for run " + runs + " is " + response.body());
                HandleException he = new HandleException("[Bookkeeping] Response message for GET req for run " + runs + " is " + response.body(), response.statusCode());
                he.isTimeToSentMail();
                return null;
            }
            body = response.body();
        } catch (Exception e) {
            logger.log(Level.WARNING, "[Bookkeeping] Communication error " + e);
            HandleException he = new HandleException("[Bookkeeping] Communication error for request " + URL_GET_RUN_INFO + encode(String.valueOf(runs))
                    + ", error message: " + e, -1);
            he.isTimeToSentMail();
            return null;
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
                                startOfDataTransfer = null, endOfDataTransfer = null, runDuration = null;
                        Double lhcBeamEnergy = null, aliceL3Current = null;
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

                        if (runInfo.getTimeO2End() != null && runInfo.getTimeO2End() > 0
                                && runInfo.getTimeO2Start() != null && runInfo.getTimeO2Start() > 0)
                            runDuration = runInfo.getTimeO2End() - runInfo.getTimeO2Start();
                        runInfo.setRunDuration(runDuration);

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

                        if (block.get("aliceL3Current") != null && block.get("aliceL3Current") instanceof Number)
                            aliceL3Current = ((Number) block.get("aliceL3Current")).doubleValue();
                        runInfo.setAliceL3Current(aliceL3Current);

                        if (block.get("definition") != null && block.get("definition") instanceof String)
                            runType = block.get("definition").toString();
                        runInfo.setRunType(runType);

                        if (block.get("lhcFill") != null && block.get("lhcFill") instanceof JSONObject) {
                            JSONObject lhcFill = (JSONObject) block.get("lhcFill");
                            if (lhcFill.get("beamType") != null && lhcFill.get("beamType") instanceof String)
                                beamType = lhcFill.get("beamType").toString();
                        }

                        if (beamType == null) {
                            if (block.get("pdpBeamType") != null && block.get("pdpBeamType") instanceof String)
                                beamType = block.get("pdpBeamType").toString();
                            else
                                beamType = "none";
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
            logger.log(Level.WARNING, "[Bookkeeping] Caught error while parsing the JSON response " + response + " " + ex);
            HandleException he = new HandleException("[Bookkeeping] Caught error while parsing the JSON response " + response + ", error message: " + ex, -1);
            he.isTimeToSentMail();
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
            logger.log(Level.WARNING, "[Bookkeeping] Caught error while parsing the JSON response " + response + " " + ex);
            HandleException he = new HandleException("[Bookkeeping] Caught error while parsing the JSON response " + response + ", error message: " + ex, -1);
            he.isTimeToSentMail();
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
            if (fields.isEmpty()) {
                logger.log(Level.WARNING, "[Bookkeeping] Could not get the required parameters for run " +  run + " from DB to send them to Bookkeeping");
                HandleException he = new HandleException("[Bookkeeping] Could not get the required parameters for run " +  run + " from DB to send them to Bookkeeping", -1);
                he.isTimeToSentMail();
                return -1;
            }

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
                    .uri(URI.create(URL_PATCH_RUN_INFO + encode(String.valueOf(run)) + "&token=" + LOGBOOOK_TOKEN_PROD))
                    .method("PATCH", HttpRequest.BodyPublishers.ofString(json))
                    .header("Content-Type", "application/json")
                    .build();

            HttpResponse<String> response =
                    httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != HttpServletResponse.SC_OK) {
                logger.log(Level.WARNING, "[Bookkeeping] Response code for PATCH req for run " + run + " is " + response.statusCode());
                logger.log(Level.WARNING, "[Bookkeeping] Response message for PATCH req for run " + run + " is " + response.body());
                logger.log(Level.INFO, json);
                HandleException he = new HandleException("[Bookkeeping] Response message for PATCH req for run " + run + " is " + response.body(), response.statusCode());
                he.isTimeToSentMail();
            }
            return response.statusCode();
        } catch (IOException | InterruptedException | UnrecoverableKeyException | KeyManagementException | NoSuchProviderException |
                 NoSuchAlgorithmException | KeyStoreException e) {
            logger.log(Level.WARNING,"[Bookkeeping] Communication error " + e);
            HandleException he = new HandleException("[Bookkeeping] Communication error for request " + URL_PATCH_RUN_INFO + encode(String.valueOf(run))
                    + ", error message: " + e, -1);
            he.isTimeToSentMail();
        }
        return -1;
    }

    public static void fetchRunInfo(Set<Long> runs) {
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
            }
        }

        if (!missingTimeO2End.isEmpty()) {
            msg = "Runs with missing timestamp information: " + missingTimeO2End + ", nr: " + missingTimeO2End.size();
            HandleException he = new HandleException("[Bookkeeping] " + msg, -1);
            he.isTimeToSentMail();
        }

        if (!missingLogbookRecord.isEmpty()) {
            msg = "Runs with missing logbook records: " + missingLogbookRecord + ", nr: " + missingLogbookRecord.size();
            HandleException he = new HandleException("[Bookkeeping] " + msg, -1);
            he.isTimeToSentMail();
        }
    }

    public static RunInfo getRunInfo(String run) {
        Set<RunInfo> runInfos = RunInfoUtils.getRunInfoFromLogBook(run);
        RunInfo runInfo = null;
        if (!runInfos.isEmpty())
            runInfo = runInfos.iterator().next();
        return runInfo;
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
                logger.log(Level.WARNING, "[Bookkeeping] Response code for GET changes req for mintime " + mintime + " and maxtime "
                        + maxtime + ": " + response.statusCode());
                logger.log(Level.WARNING, "[Bookkeeping] Response message for GET changes req for mintime " + mintime + " and maxtime "
                        + maxtime + ": " + response.body());
                HandleException he = new HandleException("[Bookkeeping] Response message for GET changes req for mintime " + mintime + " and maxtime "
                        + maxtime + ": " + response.body(), response.statusCode());
                he.isTimeToSentMail();
                return null;
            }

            runInfoSet = parseRunInfoLogsJSON(response.body());
        } catch (Exception e) {
            logger.log(Level.WARNING, "[Bookkeeping] Communication error for request " + URL_GET_CHANGES_RUN_INFO
                    + "&filter[created][from]=" + encode(String.valueOf(mintime)) + ", error message: " + e);
            HandleException he = new HandleException("[Bookkeeping] Communication error for request " + URL_GET_CHANGES_RUN_INFO
                    + "&filter[created][from]=" + encode(String.valueOf(mintime)) + ", error message: " + e, -1);
            he.isTimeToSentMail();
            return null;
        }

        return runInfoSet;
    }

    public static Set<Long> getLastUpdatedRuns(long mintime, long maxtime) {
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
        }
        return runs;
    }

    public static Integer getDaqGoodFlag(String runQuality) {
        if (runQuality == null)
            return null;
        if (runQuality.equalsIgnoreCase("bad"))
            return 0;
        if (runQuality.equalsIgnoreCase("good"))
            return 1;
        if (runQuality.equalsIgnoreCase("test"))
            return 2;
        return null;
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

    public static String getCollectionPathQuery(Long run, boolean logbookEntry) {
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

    public static Map<String, Long> getCountSizeRun(Long run) {
        DB db = new DB();
        Map<String, Long> countSizeMap = new HashMap<>();
        String select = "select count(1), sum(size) as size from rawdata where " +
                "ltrim(split_part(rawdata.lfn, '/'::text, 6), '0'::text)::integer = " + run +
                " and (status is null OR status != 'deleted');";
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

    public static Set<LFN> getLFNsFromCollection(String collectionPath) {
        Set<LFN> lfns = new HashSet<>(LFNUtils
                .find(collectionPath.replaceAll("collection", ""), "*", 0));
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

        logger.log(Level.INFO, "Lfns list size after get from collection: " + lfns.size());
        return lfns;
    }

    public static Set<LFN> getLFNsFromCollection(Long run, boolean logbookEntry) {
        DB db = new DB();
        String select = RunInfoUtils.getCollectionPathQuery(run, logbookEntry);
        Set<LFN> lfnsToProcess = new HashSet<>();
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
            lfnsToProcess.addAll(lfns);
        }

        logger.log(Level.INFO, "Lfns list size after get from collection: " + lfnsToProcess.size());
        return lfnsToProcess;
    }

    public static Set<LFN> getLFNsFromRawdataDetails(Long run, String extension) {
        DB db = new DB();
        String select = "select lfn from rawdata_details where run = " + run;

        if (extension.equalsIgnoreCase("tf"))
            select += " and lfn like '%/o2_rawtf_%.tf'";
        else if (extension.equalsIgnoreCase("ctf"))
            select += " and lfn like '%/o2_ctf_%.root'";
        else if (extension.equalsIgnoreCase("other"))
            select += " and lfn not like '%/o2_ctf_%.root' and lfn not like '%/o2_rawtf_%.tf'";
        select += ";";

        Collection<String> fileNames = new ArrayList<>();
        db.query(select);
        while (db.moveNext())
            fileNames.add(db.gets("lfn"));
        List<LFN> lfns = LFNUtils.getLFNs(true, fileNames);
        if (lfns != null && !lfns.isEmpty()) {
            logger.log(Level.INFO, "Lfns list size after get from rawdata details: " + lfns.size());
            return new HashSet<>(lfns);
        }
        return new HashSet<>();
    }

    public static Set<LFN> getLFNsFromRawdata(String startingDir) {
        DB db = new DB();
        String select = "select lfn from rawdata where lfn like '" + startingDir + "%';";
        Collection<String> fileNames = new ArrayList<>();
        db.query(select);
        while (db.moveNext())
            fileNames.add(db.gets("lfn"));
        List<LFN> lfns = LFNUtils.getLFNs(true, fileNames);
        if (lfns != null && !lfns.isEmpty()) {
            logger.log(Level.INFO, "Lfns list size after get from rawdata: " + lfns.size());
            return new HashSet<>(lfns);
        }
        return new HashSet<>();
    }

    public static void getLfnsFromCertainStorage(Set<LFN> lfns, String storage) {
        SE se = SEUtils.getSE(storage);

        Map<UUID, LFN> uuids = new HashMap<>(lfns.size());
        for (LFN l : lfns)
            uuids.put(l.guid, l);
        Set<GUID> guids = GUIDUtils.getGUIDs(uuids.keySet().toArray(new UUID[0]));

        for (GUID g : guids) {
            if (!g.hasReplica(se)) {
                LFN lfn = uuids.get(g.guid);
                if (lfn != null)
                    lfns.remove(lfn);
            }
        }

        if (lfns != null && !lfns.isEmpty())
            logger.log(Level.INFO, "Lfns list size after get from storage: " + lfns.size());
    }

    public static Set<LFN> getFirstXLfns(Set<LFN> lfns, Integer limit) {
        return lfns.stream().sorted(Comparator.comparingInt(lfn -> lfn.getCanonicalName().hashCode()))
                .limit(limit * lfns.size() / 100)
                .collect(Collectors.toCollection(HashSet::new));
    }

    public static boolean isTF(String lfnName) {
        return lfnName.matches(".*/o2_rawtf_.*\\.tf");
    }

    public static boolean isCTF(String lfnName) {
        return lfnName.matches(".*/o2_ctf_.*\\.root");
    }

    public static Map<String, Pair<Integer, Long>> getLFNsType(Set<LFN> lfns) {
        Map<String, Pair<Integer, Long>> lfnsType = new HashMap<>();
        lfnsType.put("ctf", new Pair<>(0,  0L));
        lfnsType.put("tf", new Pair<>(0,  0L));
        lfnsType.put("other", new Pair<>(0,  0L));

        for (LFN lfn : lfns) {
            if (isTF(lfn.getCanonicalName())) {
                Pair<Integer, Long> countSize = lfnsType.get("tf");
                lfnsType.put("tf", new Pair<>(countSize.getFirst() + 1, countSize.getSecond() + lfn.size));
            } else if (isCTF(lfn.getCanonicalName())) {
                Pair<Integer, Long> countSize = lfnsType.get("ctf");
                lfnsType.put("ctf", new Pair<>(countSize.getFirst() + 1, countSize.getSecond() + lfn.size));
            } else {
                Pair<Integer, Long> countSize = lfnsType.get("other");
                lfnsType.put("other", new Pair<>(countSize.getFirst() + 1, countSize.getSecond() + lfn.size));
            }
        }
        return lfnsType;
    }

    public static Map<String, Pair<Integer, Long>> getLFNsType(Long run) {
        Map<String, Pair<Integer, Long>> lfnsType = new HashMap<>();
        long ctfFileSize, tfFileSize, otherFileSize;
        int ctfFileCount, tfFileCount, otherFileCount;
        DB db = new DB();

        String select = "select tf_file_count, tf_file_size, ctf_file_count, ctf_file_size, " +
                "other_file_count, other_file_size from rawdata_runs where run=" + run + ";";
        db.query(select);
        if (!db.moveNext())
            return lfnsType;

        tfFileCount = db.geti("tf_file_count", 0);
        tfFileSize = db.getl("tf_file_size", 0);
        ctfFileCount = db.geti("ctf_file_count", 0);
        ctfFileSize = db.getl("ctf_file_size", 0);
        otherFileCount = db.geti("other_file_count", 0);
        otherFileSize = db.getl("other_file_size", 0);

        lfnsType.put("tf", new Pair<>(tfFileCount, tfFileSize));
        lfnsType.put("ctf", new Pair<>(ctfFileCount, ctfFileSize));
        lfnsType.put("other", new Pair<>(otherFileCount,otherFileSize));
        return lfnsType;
    }

    public static Map<String, Long> getReplicasForLFNs(Set<LFN> lfns) {
        Map<String, Long> seFiles = new HashMap<>();

        if (!lfns.isEmpty()) {
            Collection<UUID> uuids = new ArrayList<>(lfns.size());
            for (LFN l : lfns)
                uuids.add(l.guid);
            Set<GUID> guids = GUIDUtils.getGUIDs(uuids.toArray(new UUID[0]));

            for (GUID g : guids) {
                Set<PFN> pfns = g.getPFNs();
                for (PFN pfn : pfns) {
                    String seName = pfn.getSE().seName;
                    Long cnt = seFiles.computeIfAbsent(seName, (k) -> 0L) + 1;
                    seFiles.put(seName, cnt);
                }
            }
        }
        return seFiles;
    }

    public static String printReplicasForLFNs(Map<String, Long> seFiles) {
        String msg = "";
        for (Map.Entry<String, Long> entry : seFiles.entrySet()) {
            String seName = entry.getKey();
            Long nrFiles = entry.getValue();
            msg += seName + ": " + nrFiles + " ";
        }
        return msg.stripTrailing();
    }

    public static Map<String, Long> getReplicasForRun(Long run, String extension) {
        Set<LFN> lfns = RunInfoUtils.getLFNsFromRawdataDetails(run, extension);
        Map<String, Long> replicas = getReplicasForLFNs(lfns);
        String replicasStr = "";
        for (Map.Entry<String, Long> entry : replicas.entrySet()) {
            replicasStr += " " + entry.getKey() + " : " + entry.getValue();
        }

        if (!replicas.isEmpty())
            logger.log(Level.INFO, "Replicas for run " + run + " " + replicasStr);
        return replicas;
    }

    public static void updateReplicasForRun(Long run) {
        String ctf = String.join(" ", RunInfoUtils.getReplicasForRun(run, "ctf").keySet()).stripLeading().stripTrailing();
        String tf = String.join(" ", RunInfoUtils.getReplicasForRun(run, "tf").keySet()).stripLeading().stripTrailing();
        String other = String.join(" ", RunInfoUtils.getReplicasForRun(run, "other").keySet()).stripLeading().stripTrailing();
        DB db = new DB();

        if (ctf.isEmpty())
            ctf = null;
        if (tf.isEmpty())
            tf = null;
        if (other.isEmpty())
            other = null;

        Map<String, Object> values = new HashMap<>();
        values.put("ctf", ctf);
        values.put("tf", tf);
        values.put("other", other);
        values.put("run", run);

        String query = DBFunctions.composeUpsert("rawdata_runs_last_action", values, Set.of("run"));
        logger.log(Level.INFO, query);
        db.query(query);
    }

    public static Map<String, List<String>> getSEs() {
        Map<String, List<String>> seMap = new HashMap<>();
        seMap.put("t0", new ArrayList<>());
        seMap.put("t1", new ArrayList<>());
        seMap.put("disk", new ArrayList<>());
        seMap.put("tape", new ArrayList<>());
        for (SE se : SEUtils.getSEs(null)) {
            String name = se.getName();
            boolean t0, t1;

            t0 = name.startsWith("ALICE::CERN");
            t1 = name.startsWith("ALICE::FZK") || name.startsWith("ALICE::CNAF")
                    || name.startsWith("ALICE::CCIN2P3") || name.startsWith("ALICE::KISTI_GSDC")
                    || name.startsWith("ALICE::NDGF") || name.startsWith("ALICE::RAL")
                    || name.startsWith("ALICE::SARA") || name.startsWith("ALICE::RRC_KI_T1");

            if (t0)
                seMap.get("t0").add(name);
            else if (t1)
                seMap.get("t1").add(name);

            if (se.isQosType("disk") || se.isQosType("special"))
                seMap.get("disk").add(name);
            else if (se.isQosType("tape"))
                seMap.get("tape").add(name);
        }
        return seMap;
    }

    public static Set<String> getTier1SE() {
        Set<String> se = new HashSet<>();
        se.add("ALICE::CCIN2P3::SE");
        se.add("ALICE::CNAF::CEPH");
        se.add("ALICE::CNAF::SE");
        se.add("ALICE::FZK::SE");
        se.add("ALICE::KISTI_GSDC::EOS");
        se.add("ALICE::KISTI_GSDC::SE2");
        se.add("ALICE::NDGF::DCACHE");
        se.add("ALICE::NDGF::DCACHE_TEST");
        se.add("ALICE::RAL::CEPH");
        se.add("ALICE::RAL::CEPH_Test");
        se.add("ALICE::RRC_KI_T1::EOS");
        se.add("ALICE::SARA::DCACHE");
        se.add("ALICE::CCIN2P3::TAPE");
        se.add("ALICE::CNAF::TAPE");
        se.add("ALICE::FZK::TAPE");
        se.add("ALICE::KISTI_GSDC::CDS");
        se.add("ALICE::NDGF::DCACHE_TAPE");
        se.add("ALICE::RAL::CTA");
        se.add("ALICE::RAL::CTA_TEST");
        se.add("ALICE::RAL::Tape");
        se.add("ALICE::RRC_KI_T1::DCACHE_TAPE");
        se.add("ALICE::SARA::DCACHE_TAPE");
        return se;
    }

    public static Set<String> getTier0SE() {
        Set<String> se = new HashSet<>();
        se.add("ALICE::CERN::EOS");
        se.add("ALICE::CERN::EOSALICEO2");
        se.add("ALICE::CERN::EOSP2");
        se.add("ALICE::CERN::OCDB");
        se.add("ALICE::CERN::CTA");
        se.add("ALICE::CERN::CTA_TEST");
        return se;
    }

    public static Set<String> getDiskSE() {
        Set<String> se = new HashSet<>();
        se.add("ALICE::CCIN2P3::SE");
        se.add("ALICE::CNAF::CEPH");
        se.add("ALICE::CNAF::SE");
        se.add("ALICE::FZK::SE");
        se.add("ALICE::KISTI_GSDC::EOS");
        se.add("ALICE::KISTI_GSDC::SE2");
        se.add("ALICE::NDGF::DCACHE");
        se.add("ALICE::NDGF::DCACHE_TEST");
        se.add("ALICE::RAL::CEPH");
        se.add("ALICE::RAL::CEPH_Test");
        se.add("ALICE::RRC_KI_T1::EOS");
        se.add("ALICE::SARA::DCACHE");
        se.add("ALICE::CERN::EOS");
        se.add("ALICE::CERN::EOSALICEO2");
        se.add("ALICE::CERN::EOSP2");
        se.add("ALICE::CERN::OCDB");
        return se;
    }

    public static Set<String> getTapeSE() {
        Set<String> se = new HashSet<>();
        se.add("ALICE::CCIN2P3::TAPE");
        se.add("ALICE::CNAF::TAPE");
        se.add("ALICE::FZK::TAPE");
        se.add("ALICE::KISTI_GSDC::CDS");
        se.add("ALICE::NDGF::DCACHE_TAPE");
        se.add("ALICE::RAL::CTA");
        se.add("ALICE::RAL::CTA_TEST");
        se.add("ALICE::RAL::Tape");
        se.add("ALICE::RRC_KI_T1::DCACHE_TAPE");
        se.add("ALICE::SARA::DCACHE_TAPE");
        se.add("ALICE::CERN::CTA");
        se.add("ALICE::CERN::CTA_TEST");
        return se;
    }

    private static void printLfns(Set<LFN> lfns, String output) {
        FileWriter f = null;
        try {
            f = new FileWriter(output);
            String str = "";

            for (LFN lfn : lfns) {
                str += lfn.getCanonicalName() + "\n";
            }
            f.write(str);
            f.close();
        } catch (IOException e) {
            //todo
        }
    }

    public static void sendMail(String sTo, String sFrom, String sSubject, String sBody) {
        try {
            final Mail m = new Mail();

            m.sTo = sTo;
            m.sFrom = sFrom;
            m.sBody = sBody;
            m.sSubject = sSubject;

            final Sendmail s = new Sendmail(m.sFrom, AppConfig.getProperty("lia.util.mail.MailServer", "127.0.0.1"));
            if (!s.send(m))
                logger.log(Level.WARNING, "Could not send mail : " + s.sError);
        }
        catch (final Throwable t) {
            logger.log(Level.WARNING, "Cannot send mail", t);
        }
    }
}
