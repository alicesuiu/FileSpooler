<%@ page import="lazyj.*,alimonitor.Page,java.util.*,java.io.*,lia.Monitor.Store.Fast.DB,utils.IntervalQuery,auth.*,java.security.cert.*"%>
<%
    final ByteArrayOutputStream baos = new ByteArrayOutputStream(10000);
    final RequestWrapper rw = new RequestWrapper(request);

    final Page pMaster = new Page(baos, "WEB-INF/res/masterpage/masterpage.res");
    pMaster.comment("com_alternates", false);
    pMaster.modify("refresh_time", "300");
    pMaster.modify("comment_refresh", "//");
    pMaster.modify("title", "RAW Data Pending Deletions - Summary");

    final Page p = new Page("deletion/pending.res", false);
    final Page pLine = new Page("deletion/pending_el.res", false);

    final String sRunFilter = rw.gets("runfilter");
    final String sSourceFilter = rw.gets("sourcefilter");
    final String sStorageFilter = rw.gets("storagefilter");
    final String sPartition = rw.gets("runpartition");
    final int iStatus = rw.geti("status", 0);
    final int iAction = rw.geti("action", 0);
    final int iDataFilter = rw.geti("datafilter", 0);
    String user = null;


    pMaster.modify("bookmark", "/deletion/pending.jsp?time=0" +
            (sRunFilter.length() > 0 ? "&runfilter=" + Format.encode(sRunFilter) : "") +
            (sPartition.length() > 0 ? "&runpartition=" + Format.encode(sPartition) : "") +
            (iAction > 0 ? "&action=" + iAction : "") +
            (iDataFilter > 0 ? "&datafilter=" + iDataFilter : "") +
            (sStorageFilter.length() > 0 ? "&storagefilter=" + Format.encode(sStorageFilter) : "") +
            (sSourceFilter.length() > 0 ? "&sourcefilter=" + Format.encode(sSourceFilter) : "") +
            (iStatus > 0 ? "&status=" + iStatus : "")
    );

    boolean bAuthOK = false;
    if (request.isSecure()) {
        X509Certificate cert[] = (X509Certificate[]) request.getAttribute("javax.servlet.request.X509Certificate");
        if (cert!=null && cert.length>0) {
            AlicePrincipal principal = new AlicePrincipal(cert[0].getSubjectDN().getName());
            user = principal.getName();
            if (user!=null && user.length()>0) {
                Set<String> sRoles = LDAPHelper.checkLdapInformation("users="+user, "ou=Roles,", "uid");
                bAuthOK = sRoles.contains("rawdatamgr");
            }
        }
    }

    p.comment("com_authenticated", bAuthOK);
    p.comment("com_admin", bAuthOK);

    p.modify("runfilter", sRunFilter);
    p.modify("runpartition", sPartition);
    p.modify("action_" + iAction, "selected");
    p.modify("datafilter_" + iDataFilter, "selected");
    p.modify("storagefilter", sStorageFilter);
    p.modify("sourcefilter", sSourceFilter);
    p.modify("status_" + iStatus, "selected");

    if (bAuthOK)
        p.modify("account", user);

    String sCond = "";

    if (sRunFilter.length() > 0) {
        String q = IntervalQuery.numberInterval(sRunFilter, "run");

        if (q.length() > 0)
            sCond = IntervalQuery.cond(sCond, q);
    }

    if (sPartition.length() > 0) {
        String q = IntervalQuery.stringMatching(sPartition, "partition");

        if (q.length() > 0)
            sCond = IntervalQuery.cond(sCond, q);
    }

    if (iAction > 0) {
        sCond += (sCond.length()>0 ? " AND " : "WHERE ");

        if (iAction == 1)
            sCond += " (action='delete')";
        else if (iAction == 2)
            sCond += " (action='delete replica')";
    }

    if (iDataFilter > 0) {
        sCond += (sCond.length()>0 ? " AND " : "WHERE ");

        if (iDataFilter == 1)
            sCond += " (filter='tf')";
        else if (iDataFilter == 2)
            sCond += " (filter='ctf')";
        else if(iDataFilter == 3)
            sCond += " (filter='other')";
    }

    if (sStorageFilter.length() > 0) {
        String q = IntervalQuery.stringMatching(sStorageFilter, "sourcese");

        if (q.length() > 0)
            sCond = IntervalQuery.cond(sCond, q);
    }

    if (sSourceFilter.length() > 0) {
        String q = IntervalQuery.stringMatching(sSourceFilter, "source");

        if (q.length() > 0)
            sCond = IntervalQuery.cond(sCond, q);
    }

    if (iStatus == 1)
        sCond += (sCond.length()>0 ? " AND " : "WHERE ") + " (status='Queued')";
    else if (iStatus == 2)
        sCond += (sCond.length()>0 ? " AND " : "WHERE ") + " (status='In progress')";
    else if (iStatus == 3)
        sCond += (sCond.length()>0 ? " AND " : "WHERE ") + " (status='Inserting')";
    else if (iStatus == 4)
        sCond += (sCond.length()>0 ? " AND " : "WHERE ") + " (status='Warning')";
    else if (iStatus == 5)
        sCond += (sCond.length()>0 ? " AND " : "WHERE ") + " (status='Error')";
    else
        sCond += (sCond.length()>0 ? " AND " : "WHERE ") + " (status='Queued' OR status='In progress' OR status='Inserting' OR status='Warning' OR status='Error')";

    String select = "select id_record, run, action, filter, counter, rra.size, percentage, sourcese, source, status, addtime, log_message, chunks, rr.size as total_size, partition from rawdata_runs_action rra left outer join rawdata_runs rr using(run)" + sCond + " order by addtime desc, partition desc;";

    DB db = new DB(select);
    TreeSet<Integer> runList = new TreeSet<Integer>();

    int iRuns = 0;
    int iOldRun = 0;
    int iFiles_to_delete = 0, iFiles = 0;
    long lTotalSize_to_delete = 0, lTotalSize = 0;
    while (db.moveNext()) {
        final int iRun = db.geti("run");

        /*if (iRun == iOldRun)
            continue;*/
        runList.add(iRun);
        //iOldRun = iRun;

        pLine.modify("total_chunks", db.geti("chunks", 0));
        iFiles += db.geti("chunks", 0);
        pLine.modify("total_size", db.getl("total_size", 0));
        lTotalSize += db.getl("total_size", 0);
        pLine.modify("runpartition", db.gets("partition", ""));

        iFiles_to_delete += db.geti("counter");
        lTotalSize_to_delete += db.getl("size");
        pLine.fillFromDB(db);

        iRuns++;
        pLine.comment("com_authenticated", bAuthOK);
        p.append(pLine);
    }

    p.modify("all_runs", Format.toCommaList(runList));

    p.modify("runs", iRuns);
    p.modify("files_to_delete", iFiles_to_delete);
    p.modify("totalsize_to_delete", lTotalSize_to_delete);
    p.modify("files", iFiles);
    p.modify("totalsize", lTotalSize);

    pMaster.append(p);
    pMaster.write();

    String s = new String(baos.toByteArray());
    out.println(s);
%>