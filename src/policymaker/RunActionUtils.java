package policymaker;

import alien.config.ConfigUtils;
import lazyj.DBFunctions;
import lia.Monitor.Store.Fast.DB;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RunActionUtils {
    private static Logger logger = ConfigUtils.getLogger(RunInfoUtils.class.getCanonicalName());
    private static final String DEFAULT_SE = "ALICE::CERN::EOSALICEO2";

    private static Map<String, String> getPreviousActionInfo(Long run) {
        String select = "select ctf, tf, calib, other from rawdata_runs_last_action where run = " + run;
        DB db = new DB(select);
        String ctf = db.gets("ctf", "");
        String tf = db.gets("tf", "");
        String calib = db.gets("calib", "");
        String other = db.gets("other", "");
        Map<String, String> runActionInfo = new HashMap<>();
        runActionInfo.put("ctf", ctf);
        runActionInfo.put("tf", tf);
        runActionInfo.put("calib", calib);
        runActionInfo.put("other", other);
        return runActionInfo;
    }
    private static Map<String, Object> applyDelete(RunAction runAction) {
        String filter = runAction.getFilter();
        String action = runAction.getAction();
        Long run = runAction.getRun();
        String sourcese = runAction.getSourcese();
        Integer percentage = runAction.getPercentage();
        Map<String, Object> values = new HashMap<>();
        if (action.equalsIgnoreCase("delete replica")) {
            if (sourcese != null && sourcese.length() > 0 && (percentage == 0 || percentage == 100)) {
                Map<String, String> runActionInfo = getPreviousActionInfo(run);
                values.put("run", run);
                if (filter.equalsIgnoreCase("all")) {
                    values.put("ctf", removeWordString(runActionInfo.get("ctf"), sourcese));
                    values.put("tf", removeWordString(runActionInfo.get("tf"), sourcese));
                    values.put("calib", removeWordString(runActionInfo.get("calib"), sourcese));
                    values.put("other", removeWordString(runActionInfo.get("other"), sourcese));
                } else if (filter.equalsIgnoreCase("ctf")) {
                    values.put("ctf", removeWordString(runActionInfo.get("ctf"), sourcese));
                } else if (filter.equalsIgnoreCase("tf")) {
                    values.put("tf", removeWordString(runActionInfo.get("tf"), sourcese));
                } else if (filter.equalsIgnoreCase("calib")) {
                    values.put("calib", removeWordString(runActionInfo.get("calib"), sourcese));
                } else if (filter.equalsIgnoreCase("other")) {
                    values.put("other", removeWordString(runActionInfo.get("other"), sourcese));
                }
            }
        } else if (action.equalsIgnoreCase("delete") && (percentage == 0 || percentage == 100)) {
            values.put("run", run);
            if (filter.equalsIgnoreCase("all")) {
                values.put("ctf", null);
                values.put("tf", null);
                values.put("calib", null);
                values.put("other", null);
            } else if (filter.equalsIgnoreCase("ctf")) {
                values.put("ctf", null);
            } else if (filter.equalsIgnoreCase("tf")) {
                values.put("tf", null);
            } else if (filter.equalsIgnoreCase("calib")) {
                values.put("calib", null);
            } else if (filter.equalsIgnoreCase("other")) {
                values.put("other", null);
            }
        }
        return values;
    }

    private static String concatStrings(String s1, String s2) {
        String[] str = s2.split(" ");
        Set<String> set = new HashSet<>(Arrays.asList(str));
        StringBuilder sb = new StringBuilder();
        set.add(s1);
        for (String i : set) {
            sb.append(i).append(" ");
        }
        return sb.toString().stripTrailing();
    }

    private static String removeWordString(String s1, String s2) {
        String[] str = s1.split(" ");
        Set<String> set = new HashSet<>(Arrays.asList(str));
        StringBuilder sb = new StringBuilder();
        set.remove(s2);
        for (String i : set)
            sb.append(i).append(" ");
        return sb.toString().stripTrailing();
    }

    private static void printMap(Map<String, Object> values) {
        for (Map.Entry<String, Object> entry : values.entrySet())
            logger.log(Level.INFO, entry.getKey() + " " + entry.getValue());
    }

    private static Map<String, Object> applyCopyOrMove(RunAction runAction) {
        String filter = runAction.getFilter();
        Long run = runAction.getRun();
        String sourcese = runAction.getSourcese();
        String targetse = runAction.getTargetse();
        Integer percentage = runAction.getPercentage();
        Map<String, Object> values = new HashMap<>();
        Map<String, String> runActionInfo = getPreviousActionInfo(run);
        String ctf = runActionInfo.get("ctf"), tf = runActionInfo.get("tf"),
                calib = runActionInfo.get("calib"), other = runActionInfo.get("other");
        String action = runAction.getAction();

        if (sourcese != null && sourcese.length() > 0) {
            if (action.equalsIgnoreCase("move") && (percentage == 0 || percentage == 100)) {
                ctf = removeWordString(ctf, sourcese);
                tf = removeWordString(tf, sourcese);
                calib = removeWordString(calib, sourcese);
                other = removeWordString(other, sourcese);
            } else if (action.equalsIgnoreCase("copy")) {
                ctf = concatStrings(sourcese, ctf);
                tf = concatStrings(sourcese, tf);
                calib = concatStrings(sourcese, calib);
                other = concatStrings(sourcese, other);
            }
        } else {
            if (action.equalsIgnoreCase("move") && (percentage == 0 || percentage == 100)) {
                ctf = removeWordString(ctf, DEFAULT_SE);
                tf = removeWordString(tf, DEFAULT_SE);
                calib = removeWordString(calib, DEFAULT_SE);
                other = removeWordString(other, DEFAULT_SE);
            }
        }

        if (targetse != null && targetse.length() > 0) {
            ctf = concatStrings(targetse, ctf);
            tf = concatStrings(targetse, tf);
            calib = concatStrings(targetse, calib);
            other = concatStrings(targetse, other);
        }

        values.put("run", run);
        if (filter.equalsIgnoreCase("all")) {
            values.put("ctf", ctf);
            values.put("tf", tf);
            values.put("calib", calib);
            values.put("other", other);
        } else if (filter.equalsIgnoreCase("ctf")) {
            values.put("ctf", ctf);
        } else if (filter.equalsIgnoreCase("tf")) {
            values.put("tf", tf);
        } else if (filter.equalsIgnoreCase("calib")) {
            values.put("calib", calib);
        } else if (filter.equalsIgnoreCase("other")) {
            values.put("other", other);
        }
        return values;
    }

    public static void applyLastAction(RunAction runAction) {
        Map<String, Object> values = null;
        String action = runAction.getAction();
        Long run = runAction.getRun();
        DB db = new DB();

        if (action.equalsIgnoreCase("delete") || action.equalsIgnoreCase("delete replica"))
            values = applyDelete(runAction);
        else if (action.equalsIgnoreCase("move"))
            values = applyCopyOrMove(runAction);
        else if (action.equalsIgnoreCase("copy"))
            values = applyCopyOrMove(runAction);

        if (values != null && !values.isEmpty()) {
            String query = DBFunctions.composeUpsert("rawdata_runs_last_action", values, Set.of("run"));
            if (query == null) {
                logger.log(Level.WARNING, "insert last action for run " + run + " failed");
                return;
            }

            db.query(query);
        }
    }

    public static void retrofitRawdataRunsLastAction(Long run) {
        String select = "select run, filter, action, sourcese, targetse, percentage from rawdata_runs_action where run = " +
                run + " and status = 'Done' order by addtime;";
        DB db = new DB(select);
        while (db.moveNext()) {
            String filter = db.gets("filter", "");
            String action = db.gets("action", "");
            String sourcese = db.gets("sourcese", "");
            String targetse = db.gets("targetse", "");
            Integer percentage = db.geti("percentage", 0);

            RunAction runAction = new RunAction();
            runAction.setRun(run);
            runAction.setAction(action);
            runAction.setFilter(filter);
            runAction.setSourcese(sourcese);
            runAction.setTargetse(targetse);
            runAction.setPercentage(percentage);

            logger.log(Level.INFO, runAction.toString());
            applyLastAction(runAction);
        }
    }

    public static void applyDefaultAction(Long run) {
        Map<String, Object> values = new HashMap<>();
        DB db = new DB();

        values.put("run", run);
        values.put("ctf", DEFAULT_SE);
        values.put("tf", DEFAULT_SE);
        values.put("calib", DEFAULT_SE);
        values.put("other", DEFAULT_SE);

        String query = DBFunctions.composeInsert("rawdata_runs_last_action", values);
        if (query == null) {
            logger.log(Level.WARNING, "insert last action for run " + run + " failed");
            return;
        }
        db.query(query + " on conflict (run) do nothing");
    }

    public static long insertRunAction(Long run, String action, String filter, String source,
                                       String log_message, Integer counter, Long size, String sourcese,
                                       String targetse, String status, Integer percentage,
                                       String responsible, Long id_record) {
        Map<String, Object> values = new HashMap<>();
        values.put("filter", filter);
        values.put("counter", counter);
        values.put("size", size);
        values.put("action", action);
        values.put("run", run);
        values.put("source", source);
        values.put("log_message", log_message);
        values.put("sourcese", sourcese);
        values.put("targetse", targetse);
        values.put("status", status);
        values.put("percentage", percentage);
        values.put("responsible", responsible);

        DB db = new DB();
        String query;
        if (id_record != null) {
            values.put("id_record", id_record);
            query = DBFunctions.composeUpdate("rawdata_runs_action", values, Set.of("id_record")) + " returning id_record;";
        } else {
            query = DBFunctions.composeInsert("rawdata_runs_action", values) + " returning id_record;";
        }

        logger.log(Level.INFO, query);
        if (!db.query(query)) {
            logger.log(Level.WARNING, "Query in rawdata_runs_action failed: " + query + " " + db.getLastError());
            return -1;
        }
        return db.getl("id_record", -1);
    }
}
