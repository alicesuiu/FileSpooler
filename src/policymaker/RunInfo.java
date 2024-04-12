package policymaker;

import alien.daq.StatusDictionaryEntry;
import alien.daq.StatusDictionary;

import lazyj.DBFunctions;
import lia.Monitor.Store.Fast.DB;

import java.util.*;

public class RunInfo {
    private Long runNumber;
    private Long fillNumber;
    private Double lhcBeamEnergy;
    private String lhcBeamMode;
    private String runQuality;
    private String runType;
    private String detectors;
    private Long timeO2Start;
    private Long timeO2End;
    private Long runDuration;
    private String aliceL3Polarity;
    private String aliceDipolePolarity;
    private Double aliceL3Current;
    private String beamType;
    private Long lastModified;
    private String lhcPeriod;
    private Integer ctfFileCount;
    private Integer tfFileCount;
    private Integer otherFileCount;
    private String ctfFileSize;
    private String tfFileSize;
    private String otherFileSize;
    private Long startOfDataTransfer;
    private Long endOfDataTransfer;

    public Long getRunNumber() {
        return runNumber;
    }

    public void setRunNumber(Long runNumber) {
        this.runNumber = runNumber;
    }

    public void setFillNumber(Long fillNumber) {
        this.fillNumber = fillNumber;
    }

    public void setLhcBeamEnergy(Double lhcBeamEnergy) {
        this.lhcBeamEnergy = lhcBeamEnergy;
    }

    public void setLhcBeamMode(String lhcBeamMode) {
        this.lhcBeamMode = lhcBeamMode;
    }

    public String getRunQuality() {
        return runQuality;
    }

    public String getLhcBeamMode() {
        return lhcBeamMode;
    }

    public void setRunQuality(String runQuality) {
        this.runQuality = runQuality;
    }

    public void setRunType(String runType) {
        this.runType = runType;
    }

    public String getRunType() {
        return runType;
    }

    public void setDetectors(String detectors) {
        this.detectors = detectors;
    }

    public String getDetectors() {
        return detectors;
    }

    public Long getTimeO2Start() {
        return timeO2Start;
    }

    public void setTimeO2Start(Long timeO2Start) {
        this.timeO2Start = timeO2Start;
    }

    public Long getTimeO2End() {
        return timeO2End;
    }

    public void setTimeO2End(Long timeO2End) {
        this.timeO2End = timeO2End;
    }

    public void setRunDuration(Long runDuration) {
        this.runDuration = runDuration;
    }

    public Long getRunDuration() {
        return runDuration;
    }

    public void setAliceL3Polarity(String aliceL3Polarity) {
        this.aliceL3Polarity = aliceL3Polarity;
    }

    public void setAliceDipolePolarity(String aliceDipolePolarity) {
        this.aliceDipolePolarity = aliceDipolePolarity;
    }

    public void setAliceL3Current(Double aliceL3Current) {
        this.aliceL3Current = aliceL3Current;
    }

    public Double getAliceL3Current() {
        return aliceL3Current;
    }

    public void setBeamType(String beamType) {
        this.beamType = beamType;
    }

    public String getBeamType() {
        return beamType;
    }

    public void setLastModified(Long lastModified) {
        this.lastModified = lastModified;
    }

    public Long getLastModified() {
        return lastModified;
    }

    public String getLhcPeriod() {
        return lhcPeriod;
    }

    public void setLhcPeriod(String lhcPeriod) {
        this.lhcPeriod = lhcPeriod;
    }

    public void setCtfFileCount(Integer ctfFileCount) {
        this.ctfFileCount = ctfFileCount;
    }

    public void setTfFileCount(Integer tfFileCount) {
        this.tfFileCount = tfFileCount;
    }

    public void setOtherFileCount(Integer otherFileCount) {
        this.otherFileCount = otherFileCount;
    }

    public void setCtfFileSize(String ctfFileSize) {
        this.ctfFileSize = ctfFileSize;
    }

    public void setTfFileSize(String tfFileSize) {
        this.tfFileSize = tfFileSize;
    }

    public void setOtherFileSize(String otherFileSize) {
        this.otherFileSize = otherFileSize;
    }

    public void setStartOfDataTransfer(Long startOfDataTransfer) {
        this.startOfDataTransfer = startOfDataTransfer;
    }

    public void setEndOfDataTransfer(Long endOfDataTransfer) {
        this.endOfDataTransfer = endOfDataTransfer;
    }

    public int getPolarity() {
        String polarity;

        if (aliceL3Polarity == null) {
            polarity = "0";
        } else {
            polarity = switch (aliceL3Polarity) {
                case "NEGATIVE" -> "-";
                case "POSITIVE" -> "+";
                default -> "0";
            };
        }

        if (aliceDipolePolarity == null) {
            polarity += " 0";
        } else {
            polarity += switch (aliceDipolePolarity) {
                case "NEGATIVE" -> " -";
                case "POSITIVE" -> " +";
                default -> " 0";
            };
        }

        final StatusDictionary dict = StatusDictionary.getInstance("field");
        for (final StatusDictionaryEntry sde : dict.values()) {
            if (sde.getShortText().equals(polarity)) {
                return sde.getStatus();
            }
        }

        final StatusDictionaryEntry sde = new StatusDictionaryEntry("field", polarity, polarity, "", "LogbookSync");
        return sde.getStatus();
    }

    @Override
    public String toString() {
        return "RunInfo: {" +
                "runNumber: " + runNumber +
                ", runQuality: '" + runQuality + '\'' +
                ", runType: '" + runType + '\'' +
                ", detectors: '" + detectors + '\'' +
                ", timeO2Start: " + timeO2Start +
                ", timeO2End: " + timeO2End +
                ", runDuration: " + runDuration +
                ", fillNumber: " + fillNumber +
                ", lhcBeamEnergy: " + lhcBeamEnergy +
                ", lhcBeamMode: '" + lhcBeamMode + '\'' +
                ", beamType: '" + beamType + '\'' +
                ", aliceDipolePolarity: '" + aliceDipolePolarity + '\'' +
                ", aliceL3Polarity: '" + aliceL3Polarity + '\'' +
                ", aliceL3Current: " + aliceL3Current +
                ", startOfDataTransfer: " + startOfDataTransfer +
                ", endOfDataTransfer: " + endOfDataTransfer +
                ", ctfFileSize: '" + ctfFileSize + '\'' +
                ", ctfFileCount: " + ctfFileCount +
                ", tfFileSize: '" + tfFileSize + '\'' +
                ", tfFileCount: " + tfFileCount +
                ", otherFileSize: '" + otherFileSize + '\'' +
                ", otherFileCount: " + otherFileCount +
                '}';
    }

    public void processQuery() {
        DB db = new DB();
        String query;
        Map<String, Object> values = new HashMap<>();
        Integer daqGoodFlag = RunInfoUtils.getDaqGoodFlag(runQuality);
        int polarity = getPolarity();
        values.put("run", runNumber);
        values.put("fillno", fillNumber);
        values.put("energy", lhcBeamEnergy);
        values.put("detectors", detectors);
        if (runDuration != null && runDuration <= Integer.MAX_VALUE)
            values.put("runduration", runDuration);
        values.put("daq_time_start", timeO2Start);
        values.put("daq_time_end", timeO2End);
        values.put("lhcbeammode", lhcBeamMode);
        values.put("field", polarity);
        query = DBFunctions.composeUpsert("configuration", values, Set.of("run"));
        if (query == null)
            return;
        if (!db.query(query))
            return;

        List<String> detectorsList = new ArrayList<>();
        if (detectors != null) {
            if (detectors.contains(",")) {
                detectorsList = Arrays.asList(detectors.split(","));
            } else {
                detectorsList.add(detectors);
            }
        }

        for (String detector : detectorsList) {
            values.clear();
            values.put("run", runNumber);
            values.put("detector", detector.trim());
            values.put("status", "Pending");
            values.put("runtype", runType);
            values.put("instance", "PROD");
            query = DBFunctions.composeUpsert("shuttle", values, Set.of("run", "detector", "instance"));
            if (query == null)
                return;
            if (!db.query(query))
                return;

            values.clear();
            values.put("run", runNumber);
            values.put("detector", detector.trim());
            if (daqGoodFlag != null && Arrays.asList(0, 1, 2).contains(daqGoodFlag))
                values.put("run_quality", daqGoodFlag);
            query = DBFunctions.composeUpsert("logbook_detectors", values, Set.of("run", "detector"));
            if (query == null)
                return;
            if (!db.query(query))
                return;
        }

        values.clear();
        values.put("run", runNumber);
        values.put("detector", "SHUTTLE");
        values.put("status", "Done");
        values.put("runtype", runType);
        values.put("instance", "PROD");
        query = DBFunctions.composeUpsert("shuttle", values, Set.of("run", "detector", "instance"));
        if (query == null)
            return;
        if (!db.query(query))
            return;

        if (Arrays.asList(0, 1, 2).contains(daqGoodFlag) || runQuality.equalsIgnoreCase("none")) {
            String select = "select daq_goodflag from rawdata_runs where run = " + runNumber;
            db.query(select);
            while (db.moveNext()) {
                int existingDaqGoodFlag = db.geti("daq_goodflag", -1);
                if (Arrays.asList(0, 1, 2).contains(existingDaqGoodFlag) && Arrays.asList(0, 1, 2).contains(daqGoodFlag) && existingDaqGoodFlag != daqGoodFlag) {
                    String insert = "insert into rawdata_runs_action (run, action, filter, counter, size, source, log_message, responsible) " +
                            " select " + runNumber + ", 'change run quality', 'all', chunks, size, 'logbook', " +
                            "'run quality was changed from " + RunInfoUtils.getRunQuality(existingDaqGoodFlag) + " to " + runQuality +
                            "', 'logbook' from rawdata_runs where run=" + runNumber + ";";

                    if (!db.syncUpdateQuery(insert))
                        return;
                }
            }
            select = "select partition from rawdata_runs where run = " + runNumber;
            db.query(select);
            while (db.moveNext()) {
                String partition = db.gets(1);
                values.clear();
                values.put("daq_goodflag", daqGoodFlag);
                values.put("daq_transfercomplete", 1);
                values.put("run", runNumber);
                values.put("partition", partition);
                values.put("beamtype", beamType);
                query = DBFunctions.composeUpsert("rawdata_runs", values, Set.of("run", "partition"));
                if (query == null)
                    return;
                if (!db.query(query))
                    return;
            }
        }

        RunActionUtils.applyDefaultAction(runNumber);
    }
}
