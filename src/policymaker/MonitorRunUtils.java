package policymaker;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lia.Monitor.Store.Cache;
import lia.Monitor.monitor.Result;
import lia.Monitor.monitor.TimestampedResult;
import lia.Monitor.monitor.monPredicate;

public class MonitorRunUtils {
    private static final String SFARM = "alicdb3.cern.ch";

    public static Map<Long, MonitorRun> getActiveRuns() {
        monPredicate predRuns = new monPredicate(SFARM, "epn2eos", "*", -900000, -1, new String[] { "*" }, null);
        final List<TimestampedResult> activeRuns = Cache.filterByTime(Cache.getLastValues(predRuns), predRuns);
        Map<Long, MonitorRun> activeMonitorRuns = new HashMap<>();

        if (activeRuns != null) {
            for (final TimestampedResult tr : activeRuns) {
                if (tr instanceof Result) {
                    Result res = (Result) tr;
                    final long run = Long.parseLong(res.param_name[0]);
                    long value = Math.round(res.param[0]);

                    MonitorRun mr = activeMonitorRuns.computeIfAbsent(run, (k) -> new MonitorRun(k, 0, 0, 0, 0, 0, 0));
                    if (res.NodeName.endsWith("_cnt") && value > 0) {
                        mr.setCnt(mr.getCnt() + value);
                        mr.setNodes(mr.getNodes() + 1);
                    }
                    else
                    if (res.NodeName.endsWith("_size")) {
                        mr.setSize(mr.getSize() + value);
                    }
                    activeMonitorRuns.put(run, mr);
                }
            }
        }
        return activeMonitorRuns;
    }
}
