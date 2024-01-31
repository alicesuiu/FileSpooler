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

        if (mr.getCnt() > 0)
            p.modify("cnt", mr.getCnt());

        if (mr.getSize() > 0)
            p.modify("size", mr.getSize());

        if (mr.getNodes() > 0)
            p.modify("epns", mr.getNodes());

        if (mr.getEta() > 0)
            p.modify("eta", mr.getEta());

        p.modify("last_seen", mr.getLastSeen());

        long delay = mr.getLastSeen() - mr.getMaxTime();
        p.modify("delay", delay);
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
    pMaster.modify("title", "EPN2EOS - List of runs that are transferred to storage");

    final Page p = new Page("epn2eos_active_runs/active_runs.res", false);
    final Page pLine = new Page("epn2eos_active_runs/active_runs_el.res", false);

    long total_files = 0, total_registered_files = 0;
    long total_size = 0, total_registered_size = 0;
    double global_eta = 0;
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
            db.query("select pn2eos_end_time from rawdata_runs where run=" + mr.getRun() + ";");
            mr.setLastSeen(db.getl("epn2eos_end_time", 0));
            mr.setNodes(0);
        } else {
            mr.setLastSeen(System.currentTimeMillis() / 1000);
        }

        db.query("select maxtime from rawdata_runs where run= " + mr.getRun() + ";");
        mr.setMaxTime(db.getl("maxtime", 0));

        if (global_eta < mr.getEta())
            global_eta = mr.getEta();
    }

    list = new LinkedList<>(activeMonitorRuns.values());
    list.sort(Comparator.comparingLong(MonitorRun::getRun));

    for (MonitorRun mr : list) {
        fillPage(pLine, mr);
        db.query("select chunks, size from rawdata_runs where run= " + mr.getRun() + ";");

        long chunks = db.getl("chunks", 0);
        long size = db.getl("size", 0);

        if (chunks > 0) {
            pLine.modify("registered_cnt", chunks);
            total_registered_files += chunks;
        }

        if (size > 0) {
            pLine.modify("registered_size", size);
            total_registered_size += size;
        }

        p.append(pLine);
    }

    p.modify("total_runs", total_runs);

    if (total_files > 0)
        p.modify("total_files", total_files);

    if (total_size > 0)
        p.modify("total_size", total_size);

    if (global_eta > 0)
        p.modify("total_eta", global_eta);

    if (total_registered_files > 0)
        p.modify("total_registered_files", total_registered_files);

    if (total_registered_size > 0)
        p.modify("total_registered_size", total_registered_size);

    p.modify("global_rate", rate);

    pMaster.append(p);
    pMaster.write();

    final String s = new String(baos.toByteArray());
    out.println(s);

    lia.web.servlets.web.Utils.logRequest("/epn2eos_active_runs/active_runs.jsp", baos.size(), request);
%>