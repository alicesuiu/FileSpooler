<%@ page import="lazyj.*,alimonitor.Page,java.util.*,java.io.*,lia.Monitor.Store.Fast.DB,utils.IntervalQuery,auth.*,java.security.cert.*"%>
<%@ page import="org.glite.security.util.DN" %>
<%
    final ByteArrayOutputStream baos = new ByteArrayOutputStream(10000);
    final RequestWrapper rw = new RequestWrapper(request);

    final Page pMaster = new Page(baos, "WEB-INF/res/masterpage/masterpage.res");
    pMaster.comment("com_alternates", false);
    pMaster.modify("refresh_time", "300");
    pMaster.modify("comment_refresh", "//");
    pMaster.modify("title", "RAW Data Deletion Actions - Summary");

    final Page p = new Page("deletion/index.res", false);
    final Page pLine = new Page("deletion/index_el.res", false);

    final String sRunFilter = rw.gets("runfilter");
    final String sTypeFilter = rw.gets("typefilter");
    final String sSourceFilter = rw.gets("sourcefilter");
    final String sStorageFilter = rw.gets("storagefilter");
    final int iStatus = rw.geti("status", 0);
    final int iAction = rw.geti("action", 0);


    pMaster.modify("bookmark", "/deletion/?time=0" +
            (sRunFilter.length() > 0 ? "&runfilter=" + Format.encode(sRunFilter) : "") +
            (iAction > 0 ? "&action=" + iAction : "") +
            (sTypeFilter.length() > 0 ? "&typefilter=" + Format.encode(sTypeFilter) : "") +
            (sStorageFilter.length() > 0 ? "&storagefilter=" + Format.encode(sStorageFilter) : "") +
            (sSourceFilter.length() > 0 ? "&sourcefilter=" + Format.encode(sSourceFilter) : "") +
            (iStatus > 0 ? "&status=" + iStatus : "")
    );

    boolean bAuthOK = false;

    if (request.isSecure()) {
        X509Certificate cert[] = (X509Certificate[]) request.getAttribute("javax.servlet.request.X509Certificate");
        if (cert!=null && cert.length>0) {
            AlicePrincipal principal = new AlicePrincipal(cert[0].getSubjectDN().getName());
            String sName = principal.getName();
            if (sName!=null && sName.length()>0) {
                Set<String> sRoles = LDAPHelper.checkLdapInformation("users="+sName, "ou=Roles,", "uid");
                bAuthOK = sRoles.contains("rawdatamgr");
            }
        }
    }

    p.comment("com_authenticated", !bAuthOK);
    p.comment("com_admin", bAuthOK);

    p.modify("runfilter", sRunFilter);
    p.modify("action_" + iAction, "selected");
    p.modify("typefilter", sTypeFilter);
    p.modify("storagefilter", sStorageFilter);
    p.modify("sourcefilter", sSourceFilter);
    p.modify("status_" + iStatus, "selected");

    String sCond = "";

    if (sRunFilter.length() > 0) {
        String q = IntervalQuery.numberInterval(sRunFilter, "run");

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

    if (sTypeFilter.length() > 0) {
        String q = IntervalQuery.stringMatching(sTypeFilter, "filter");

        if (q.length() > 0)
            sCond = IntervalQuery.cond(sCond, q);
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
    else
        sCond += (sCond.length()>0 ? " AND " : "WHERE ") + " (status='Queued' or status='In progress')";

    String select = "select run, action, filter, counter, size, percentage, sourcese, source, status, addtime from rawdata_runs_action " + sCond + " order by addtime desc;";

    DB db = new DB(select);
    DB db1 = new DB();
    TreeSet<Integer> runList = new TreeSet<Integer>();

    int iRuns = 0;
    int iOldRun = 0;
    int iFiles_to_delete = 0, iFiles = 0;
    long lTotalSize_to_delete = 0, lTotalSize = 0;
    while (db.moveNext()) {
        final int iRun = db.geti("run");

        if (iRun == iOldRun)
            continue;
        runList.add(iRun);
        iOldRun = iRun;

        select = "select chunks, size from rawdata_runs where run = " + iRun + ";";
        db1.query(select);
        pLine.modify("total_chunks", db1.geti("chunks", 0));
        iFiles += db1.geti("chunks", 0);
        pLine.modify("total_size", db1.getl("size", 0));
        lTotalSize += db1.getl("size", 0);

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