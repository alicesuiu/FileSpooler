<%@ page import="lia.Monitor.monitor.monPredicate" %>
<%@ page import="lia.Monitor.Store.Cache" %>
<%@ page import="lia.Monitor.monitor.TimestampedResult" %>
<%@ page import="lia.Monitor.monitor.Result" %>
<%@ page import="java.io.ByteArrayOutputStream" %>
<%@ page import="alimonitor.Page" %>
<%@ page import="java.util.*" %>
<%!
    private static final String SFARM = "alicdb3.cern.ch";
    private static final class MonitorRun {
        String run;
        long cnt;
        long size;
        double eta;

        public MonitorRun(String run, long cnt, long size, double eta) {
            this.run = run;
            this.cnt = cnt;
            this.size = size;
            this.eta = eta;
        }

        public void fillPage(final Page p){
            p.modify("run", run);
            p.modify("cnt", cnt);
            p.modify("size", size);
            p.modify("eta", eta);
        }
    }
%>

<%
    monPredicate predRuns = new monPredicate(SFARM, "epn2eos", "*", -1, -1, new String[] {"*"}, null);
    final Vector<TimestampedResult> activeRuns = Cache.getLastValues(predRuns);
    Map<String, MonitorRun> activeMonitorRuns = new TreeMap<>();

    if (activeRuns != null) {
        for (final TimestampedResult tr : activeRuns) {
            if (tr instanceof Result) {
                Result res = (Result) tr;
                final String run = res.param_name[0];
                long value = Math.round(res.param[0]);

                MonitorRun mr = activeMonitorRuns.computeIfAbsent(run, (k) -> new MonitorRun(k, 0, 0, 0));
                if (res.NodeName.endsWith("_cnt")) {
                    mr.cnt += value;
                } else if (res.NodeName.endsWith("_size")) {
                    mr.size += value;
                }
                activeMonitorRuns.put(run, mr);
            }
        }
    }

    monPredicate predRate = new monPredicate(SFARM, "ALIEN_spooler.Spooler_Nodes_Summary", "sum",
            -1, -1, new String[] {"nr_transmitted_bytes_R"}, null);
    final TimestampedResult tr = Cache.getLastValue(predRate);
    Result res = (Result) tr;
    final double rate;
    if (res != null && res.param[0] >= 1)
        rate = res.param[0];
    else
        rate = 30L * 1024 * 1024 * 1024;

    final ByteArrayOutputStream baos = new ByteArrayOutputStream(10000);
    final Page pMaster = new Page(baos, "WEB-INF/res/masterpage/masterpage.res", false);
    pMaster.comment("com_alternates", false);
    pMaster.modify("refresh_time", 120);
    pMaster.modify("title", "EPN2EOS - List of active runs");

    final Page p = new Page("epn2eos_active_runs/active_runs.res", false);
    final Page pLine = new Page("epn2eos_active_runs/active_runs_el.res", false);

    long total_files = 0;
    long total_size = 0;
    List<MonitorRun> list = new LinkedList<>(activeMonitorRuns.values());
    list.sort(Comparator.comparingLong(elem -> elem.size));
    final long total_runs = list.size();
    while (!list.isEmpty()) {
        long nr_runs = list.size();
        double mr_rate = rate / nr_runs;
        MonitorRun mr = list.remove(0);
        total_files += mr.cnt;
        total_size += mr.size;
        mr.eta += mr.size / mr_rate;

        for (MonitorRun mr1 : list) {
            mr1.size -= mr.size;
            mr1.eta = mr.eta;
        }
    }

    for (MonitorRun mr : activeMonitorRuns.values()) {
        mr.fillPage(pLine);
        p.append(pLine);
    }

    final double total_eta = total_size / rate;
    p.modify("total_runs", total_runs);
    p.modify("total_files", total_files);
    p.modify("total_size", total_size);
    p.modify("total_eta", total_eta);

    pMaster.append(p);
    pMaster.write();

    final String s = new String(baos.toByteArray());
    out.println(s);

    lia.web.servlets.web.Utils.logRequest("/epn2eos_active_runs/active_runs.jsp", baos.size(), request);
%>