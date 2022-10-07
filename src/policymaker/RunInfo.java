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

    public Long getRunNumber() {
        return runNumber;
    }

    public void setRunNumber(Long runNumber) {
        this.runNumber = runNumber;
    }

    public Long getFillNumber() {
        return fillNumber;
    }

    public void setFillNumber(Long fillNumber) {
        this.fillNumber = fillNumber;
    }

    public Double getLhcBeamEnergy() {
        return lhcBeamEnergy;
    }

    public void setLhcBeamEnergy(Double lhcBeamEnergy) {
        this.lhcBeamEnergy = lhcBeamEnergy;
    }

    public String getLhcBeamMode() {
        return lhcBeamMode;
    }

    public void setLhcBeamMode(String lhcBeamMode) {
        this.lhcBeamMode = lhcBeamMode;
    }

    public String getRunQuality() {
        return runQuality;
    }

    public void setRunQuality(String runQuality) {
        this.runQuality = runQuality;
    }

    public String getRunType() {
        return runType;
    }

    public void setRunType(String runType) {
        this.runType = runType;
    }

    public String getDetectors() {
        return detectors;
    }

    public void setDetectors(String detectors) {
        this.detectors = detectors;
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

    public Long getRunDuration() {
        return runDuration;
    }

    public void setRunDuration(Long runDuration) {
        this.runDuration = runDuration;
    }

    public String getAliceL3Polarity() {
        return aliceL3Polarity;
    }

    public void setAliceL3Polarity(String aliceL3Polarity) {
        this.aliceL3Polarity = aliceL3Polarity;
    }

    public String getAliceDipolePolarity() {
        return aliceDipolePolarity;
    }

    public void setAliceDipolePolarity(String aliceDipolePolarity) {
        this.aliceDipolePolarity = aliceDipolePolarity;
    }

    public int getDaqGoodFlag() {
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
                ", aliceDipolePolarity: '" + aliceDipolePolarity + '\'' +
                ", aliceL3Polarity: '" + aliceL3Polarity + '\'' +
                '}';
    }

    public void processQuery() {
        DB db = new DB();
        String query;
        int daqGoodFlag = getDaqGoodFlag();
        Map<String, Object> values = new HashMap<>();
        if (daqGoodFlag >= 0) {
            String select = "select partition from rawdata_runs where run = " + runNumber;
            db.query(select);
            String partition = db.gets(1);
            values.put("daq_goodflag", daqGoodFlag);
            //values.put("daq_transfercomplete", 1);
            values.put("run", runNumber);
            values.put("partition", partition);
            query = DBFunctions.composeUpsert("rawdata_runs", values, Set.of("run", "partition"));
            //RunInfoUtils.logMessage(query);
            if (query != null)
                db.query(query);
        }

        int polarity = getPolarity();
        values.clear();
        values.put("run", runNumber);
        values.put("fillno", fillNumber);
        values.put("energy", lhcBeamEnergy);
        values.put("detectors", detectors);
        values.put("runduration", runDuration);
        values.put("daq_time_start", timeO2Start);
        values.put("daq_time_end", timeO2End);
        values.put("lhcbeammode", lhcBeamMode);
        values.put("field", polarity);
        query = DBFunctions.composeUpsert("configuration", values, Set.of("run"));
        //RunInfoUtils.logMessage(query);
        if (query != null)
            db.query(query);

        List<String> detectorsList = new ArrayList<>();
        if (detectors.contains(",")) {
            detectorsList = Arrays.asList(detectors.split(","));
        } else {
            detectorsList.add(detectors);
        }

        for (String detector : detectorsList) {
            values.clear();
            values.put("run", runNumber);
            values.put("detector", detector.trim());
            values.put("status", "Pending");
            values.put("runtype", runType);
            values.put("instance", "PROD");
            query = DBFunctions.composeUpsert("shuttle", values, Set.of("run", "detector", "instance"));
            //RunInfoUtils.logMessage(query);
            if (query != null)
                db.query(query);

            values.clear();
            values.put("run", runNumber);
            values.put("detector", detector.trim());
            if (daqGoodFlag >= 0)
                values.put("run_quality", daqGoodFlag);
            query = DBFunctions.composeUpsert("logbook_detectors", values, Set.of("run", "detector"));
            //RunInfoUtils.logMessage(query);
            if (query != null)
                db.query(query);
        }

        values.clear();
        values.put("run", runNumber);
        values.put("detector", "SHUTTLE");
        values.put("status", "Done");
        values.put("runtype", runType);
        values.put("instance", "PROD");
        query = DBFunctions.composeUpsert("shuttle", values, Set.of("run", "detector", "instance"));
        //RunInfoUtils.logMessage(query);
        if (query != null)
            db.query(query);
    }
}
