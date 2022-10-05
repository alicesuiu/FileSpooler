<%@ page import="lia.Monitor.monitor.monPredicate" %>
<%@ page import="lia.Monitor.monitor.TimestampedResult" %>
<%@ page import="lia.Monitor.Store.Cache" %>
<%@ page import="java.util.Vector" %>
<%@ page import="java.util.Map" %>
<%@ page import="lia.Monitor.monitor.Result" %>
<%@ page import="org.json.simple.JSONObject" %>
<%@ page import="org.json.simple.JSONArray" %>
<%@ page import="java.util.HashMap" %>
<%!
    private static final String SFARM = "alicdb3.cern.ch";
%>
<%
    monPredicate predRuns = new monPredicate(SFARM, "epn2eos", "*", -1, -1, new String[] {"*"}, null);
    final Vector<TimestampedResult> activeRuns = Cache.getLastValues(predRuns);
    Map<String, Long> activeMonitorRuns = new HashMap<>();

    if (activeRuns != null) {
        for (final TimestampedResult tr : activeRuns) {
            if (tr instanceof Result) {
                Result res = (Result) tr;
                final String run = res.param_name[0];
                long value = Math.round(res.param[0]);

                long cnt = activeMonitorRuns.computeIfAbsent(run, (k) -> 0L);
                if (res.NodeName.endsWith("_cnt")) {
                    cnt += value;
                }
                activeMonitorRuns.put(run, cnt);
            }
        }
    }

    response.setContentType("application/json");
    JSONObject json = new JSONObject();
    JSONArray data = new JSONArray();
    for (Map.Entry<String, Long> entry : activeMonitorRuns.entrySet()) {
        JSONObject item = new JSONObject();
        item.put("run", entry.getKey());
        item.put("cnt", entry.getValue());
        data.add(item);
    }

    json.put("data", data);
    String message = json.toString();
    out.println(message);
%>