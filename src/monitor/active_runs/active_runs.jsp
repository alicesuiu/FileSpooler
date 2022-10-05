<%@ page import="alimonitor.Page" %>
<%@ page import="policymaker.MonitorRun" %>
<%@ page import="policymaker.MonitorRunUtils" %>
<%@ page import="lia.Monitor.monitor.monPredicate" %>
<%@ page import="lia.Monitor.monitor.TimestampedResult" %>
<%@ page import="java.util.*" %>
<%@ page import="lia.Monitor.Store.Fast.DB" %>
<%@ page import="lia.Monitor.Store.Cache" %>
<%@ page import="lia.Monitor.monitor.Result" %>
<%@ page import="java.io.ByteArrayOutputStream" %>
<%!
    private void fillPage(final Page p, MonitorRun mr){
        p.modify("run", mr.getRun());
        p.modify("cnt", mr.getCnt());
        p.modify("size", mr.getSize());
        p.modify("eta", mr.getEta());
        p.modify("last_seen", mr.getLastSeen());
    }
%>

<%
    Map<Long, MonitorRun> activeMonitorRuns = MonitorRunUtils.getActiveRuns();

    monPredicate predRate = new monPredicate("alicdb3.cern.ch", "ALIEN_spooler.Spooler_Nodes_Summary", "sum",
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
    list.sort(Comparator.comparingLong(MonitorRun::getSize));
    final long total_runs = list.size();
    DB db = new DB();
    while (!list.isEmpty()) {
        long nr_runs = list.size();
        double mr_rate = rate / nr_runs;
        MonitorRun mr = list.remove(0);
        total_files += mr.getCnt();
        total_size += mr.getSize();
        double new_eta = mr.getEta() + mr.getSize() / mr_rate;
        mr.setEta(new_eta);

        for (MonitorRun mr1 : list) {
            mr1.setSize(mr1.getSize() - mr.getSize());
            mr1.setEta(mr.getEta());
        }

        if (mr.getCnt() == 0) {
            db.query("select maxtime from rawdata_runs where run=" + mr.getRun() + ";");
            mr.setLastSeen(db.geti(1));
        } else {
            mr.setLastSeen(System.currentTimeMillis());
        }
    }

    for (MonitorRun mr : activeMonitorRuns.values()) {
        fillPage(pLine, mr);
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