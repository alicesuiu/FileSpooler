package policymaker;

import alien.user.JAKeyStore;
import lazyj.Format;
import lia.Monitor.Store.Fast.DB;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.net.*;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

public class RunInfoUtils {
    //private static String URL_GET_RUN_INFO = "https://ali-bookkeeping.cern.ch/api/runs?filter[runNumbers]=";
    private static String URL_PATCH_RUN_INFO = "http://guis-main.cern.ch:4000/api/runs/?runNumber=";
    private static String URL_GET_RUN_INFO = "http://guis-main.cern.ch:4000/api/runs/?filter[runNumbers]=";

    private static String encode(final String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }

    public static Set<RunInfo> getRunInfoFromLogBook(String runs) {
        Set<RunInfo> runInfoSet = new HashSet<>();
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

            HttpResponse<String> response =
                    httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != HttpServletResponse.SC_OK)
                return runInfoSet;

            JSONObject result = (JSONObject) new JSONParser().parse(response.body());

            if (result != null) {
                if (result.get("data") != null && result.get("data") instanceof JSONArray) {
                    JSONArray data = (JSONArray) result.get("data");
                    for (Object o : data) {
                        String runQuality = null, runType = null, detectors = null, lhcBeamMode = null,
                                aliceL3Polarity = null, aliceDipolePolarity = null, beamType = null;
                        Long timeO2Start = null, timeO2End = null, runNumber = null, fillNumber = null;
                        Double lhcBeamEnergy = null;
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

                        if (block.get("timeO2Start") != null && block.get("timeO2Start") instanceof Number)
                            timeO2Start = ((Number) block.get("timeO2Start")).longValue();
                        runInfo.setTimeO2Start(timeO2Start);

                        if (block.get("timeO2End") != null && block.get("timeO2End") instanceof Number)
                            timeO2End = ((Number) block.get("timeO2End")).longValue();
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
                        runInfoSet.add(runInfo);

                        /*if (block.get("lhcFill") != null && block.get("lhcFill") instanceof JSONObject) {
                            JSONObject lhcFill = (JSONObject) block.get("lhcFill");
                            if (lhcFill.get("beamType") != null && lhcFill.get("beamType") instanceof String)
                                beamType = lhcFill.get("beamType").toString();
                            logMessage("beam type for run " + runs + " is " + beamType);
                        }*/
                    }
                }
            }
        } catch (IOException e) {
            logMessage("Communication error " + e.getMessage());
        } catch (Exception e) {
            logMessage("Caught error while parsing the JSON response for run list " +  runs +  " " + e.getMessage());
        }
        return runInfoSet;
    }

    public static int sendRunInfoToLogBook(Long run, Map<String, Object> fields) {
        try {
            String json = Format.toJSON(fields).toString();
            HttpClient httpClient = HttpClient.newBuilder()
                    .version(HttpClient.Version.HTTP_2)
                    .build();

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(URL_PATCH_RUN_INFO + encode(String.valueOf(run))))
                    .method("PATCH", HttpRequest.BodyPublishers.ofString(json))
                    .header("Content-Type", "application/json")
                    .build();

            HttpResponse<String> response =
                    httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            logMessage("Response code for PATCH req for run " + run + " is " + response.statusCode());
            if (response.statusCode() != HttpServletResponse.SC_OK) {
                logMessage("Response message for PATCH req for run " + run + " is " + response.body());
                logMessage(json);
            }
            return response.statusCode();
        } catch (IOException | InterruptedException e) {
            logMessage("Communication error " + e.getMessage());
        }
        return -1;
    }

    public static void setRunInfo(long mintime, long maxtime) {
        String select = "select run from rawdata_runs where mintime >= " + mintime + " and maxtime <= " + maxtime;
        //+ " and daq_transfercomplete IS NULL limit 1;";

        List<Long> dbRunsList = getSetOfRunsFromCertainSelect(select);
        List<Long> missingTimeO2End = new ArrayList<>();
        List<Long> missingQualityFlag = new ArrayList<>();
        for (Long run : dbRunsList) {
            Map<String, Object> fields = getRunParamsForLogBook(String.valueOf(run));
            int status = sendRunInfoToLogBook(run, fields);
            if (status == HttpServletResponse.SC_OK) {
                Set<RunInfo> runInfos = RunInfoUtils.getRunInfoFromLogBook(String.valueOf(run));
                if (!runInfos.isEmpty()) {
                    RunInfo runInfo = runInfos.iterator().next();
                    if (runInfo.getRunNumber() == null) {
                        logMessage("There is no record for run " + run + " in the Logbook");
                        return;
                    }

                    if (runInfo.getDaqGoodFlag() < 0) {
                        missingQualityFlag.add(run);
                    }

                    if (runInfo.getTimeO2End() != null && runInfo.getTimeO2End() > 0) {
                        logMessage(runInfo.toString());
                        // todo: compute runDuration
                        runInfo.processQuery();
                    } else {
                        missingTimeO2End.add(run);
                    }
                }
            } else {
                logMessage("The PATCH request to the logbook did not work. We caught HTTP error code: " + status);
            }
        }
        logMessage("List of runs that have timeO2end null or 0 " + missingTimeO2End);
        logMessage("List of runs that do not have the run quality flag set " + missingQualityFlag);
    }

    private static Map<String, Object> getRunParamsForLogBook(String run) {
        Map<String, Object> fields = new HashMap<>();
        long startOfDataTransfer, endOfDataTransfer, ctfFileSize, tfFileSize, otherFileSize;
        int ctfFileCount, tfFileCount, otherFileCount;
        DB db = new DB();

        String select = "select mintime, maxtime, tf_file_count, tf_file_size, ctf_file_count, ctf_file_size, " +
                "other_file_count, other_file_size from rawdata_runs where run=" + run + ";";
        db.query(select);
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

    private static List<Long> getSetOfRunsFromCertainSelect(String select) {
        Map<Long, MonitorRun> activeRunsMap = MonitorRunUtils.getActiveRuns();
        Set<Long> activeRunsSet = new HashSet<>();
        for (Map.Entry<Long, MonitorRun> entry : activeRunsMap.entrySet()) {
            if (entry.getValue().getCnt() != 0) {
                activeRunsSet.add(entry.getKey());
            }
        }

        List<Long> dbRunsList = new ArrayList<>();
        DB db = new DB();
        db.query(select);
        while (db.moveNext()) {
            long run = db.geti(1);
            dbRunsList.add(run);
        }
        dbRunsList.removeAll(activeRunsSet);
        return dbRunsList;
    }
    public static void retrofitCountersInRawdataRuns(long mintime, long maxtime) {
        long ctfFileSize, tfFileSize, otherFileSize;
        int ctfFileCount, tfFileCount, otherFileCount;

        String select = "select run from rawdata_runs where mintime >= " + mintime + " and maxtime <= " + maxtime;
        List<Long> dbRunsList = getSetOfRunsFromCertainSelect(select);
        DB db = new DB();

       for (Long run : dbRunsList) {
            String selectCTF = "select count(size), sum(size) from rawdata_details where run=" + run + " and lfn like '%/o2_ctf_%.root';";
            db.query(selectCTF);
            ctfFileCount = db.geti(1);
            ctfFileSize = db.getl(2);

            String selectTF = "select count(size), sum(size) from rawdata_details where run=" + run + " and lfn like '%/o2_rawtf_%.tf';";
            db.query(selectTF);
            tfFileCount = db.geti(1);
            tfFileSize = db.getl(2);

            String selectOther = "select count(size), sum(size) from rawdata_details where run=" + run
                    + " and lfn not like '%/o2_ctf_%.root' and lfn not like '%/o2_rawtf_%.tf';";
            db.query(selectOther);
            otherFileCount = db.geti(1);
            otherFileSize = db.getl(2);

            String update = "UPDATE rawdata_runs SET tf_file_count=" + tfFileCount + ", tf_file_size=" + tfFileSize
                    + ", ctf_file_count=" + ctfFileCount + ", ctf_file_size=" + ctfFileSize + ", other_file_count="
                    + otherFileCount + ", other_file_size=" + otherFileSize + " WHERE run=" + run + ";";

            db.syncUpdateQuery(update);
        }
    }

    public static void logMessage(final String message) {
        String filePath = "/home/monalisa/MLrepository/lib/classes/asuiu/runInfo.log";
        try (PrintWriter pwLog = new PrintWriter(new FileWriter(filePath, true))) {
            pwLog.println(message);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
