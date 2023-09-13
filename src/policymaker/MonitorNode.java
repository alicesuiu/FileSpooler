package policymaker;

import alien.config.ConfigUtils;
import lazyj.DBFunctions;
import lia.Monitor.Store.Fast.DB;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MonitorNode implements Comparable<MonitorNode> {
    private String epn;
    private boolean production;
    private boolean uptime;
    private final Logger logger = ConfigUtils.getLogger(MonitorNode.class.getCanonicalName());

    public MonitorNode(String epn, boolean production, boolean uptime) {
        this.epn = epn;
        this.production = production;
        this.uptime = uptime;
    }

    public String getEpn() {
        return epn;
    }

    public void setEpn(String epn) {
        this.epn = epn;
    }

    public boolean isProduction() {
        return production;
    }

    public void setProduction(boolean production) {
        this.production = production;
    }

    public boolean isUptime() {
        return uptime;
    }

    public void setUptime(boolean uptime) {
        this.uptime = uptime;
    }

    public void updateNodeDB() {
        DB db = new DB();
        Map<String, Object> values = new HashMap<>();

        values.put("epn", epn);
        values.put("uptime", uptime);
        values.put("production", production);
        values.put("addtime", (System.currentTimeMillis() / 1000));
        String update = DBFunctions.composeUpsert("epn2eos_status", values, Set.of("epn"));
        if (!db.query(update))
            logger.log(Level.WARNING, "Upsert in epn2eos_status failed for node: " + epn + " " + db.getLastError());
    }

    @Override
    public int compareTo(MonitorNode o) {
        return epn.compareTo(o.epn);
    }

    @Override
    public String toString() {
        return "MonitorNode{" +
                "epn='" + epn + '\'' +
                ", production=" + production +
                ", uptime=" + uptime +
                '}';
    }
}
