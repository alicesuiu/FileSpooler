<%@ page import="lazyj.*,alimonitor.Page,java.util.*,java.io.*,lia.Monitor.Store.Fast.DB,utils.IntervalQuery,auth.*,java.security.cert.*"%>
<%
    final ByteArrayOutputStream baos = new ByteArrayOutputStream(10000);
    final RequestWrapper rw = new RequestWrapper(request);

    final Page pMaster = new Page(baos, "WEB-INF/res/masterpage/masterpage.res");
    pMaster.comment("com_alternates", false);
    pMaster.modify("refresh_time", "300");
    pMaster.modify("comment_refresh", "//");
    pMaster.modify("title", "RAW Data Already Removed - Summary");

    final Page p = new Page("deletion/taken.res", false);
    final Page pLine = new Page("deletion/taken_el.res", false);

    final String sRunFilter = rw.gets("runfilter");
    final String sPartition = rw.gets("runpartition");
    final String sReqFilter = rw.gets("reqfilter");
    final String sRespFiler = rw.gets("respfilter");
    final String sStorageFilter = rw.gets("storagefilter");
    final int iAction = rw.geti("action", 0);
    final int iDataFilter = rw.geti("datafilter", 0);
    String user = null;

    pMaster.modify("bookmark", "/deletion/taken.jsp?time=0" +
            (sRunFilter.length() > 0 ? "&runfilter=" + Format.encode(sRunFilter) : "") +
            (sPartition.length() > 0 ? "&runpartition=" + Format.encode(sPartition) : "") +
            (iAction > 0 ? "&action=" + iAction : "") +
            (iDataFilter > 0 ? "&datafilter=" + iDataFilter : "") +
            (sStorageFilter.length() > 0 ? "&storagefilter=" + Format.encode(sStorageFilter) : "") +
            (sReqFilter.length() > 0 ? "&reqfilter=" + Format.encode(sReqFilter) : "") +
            (sRespFiler.length() > 0 ? "&respfilter=" + Format.encode(sRespFiler) : "")
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
    p.modify("reqfilter", sReqFilter);
    p.modify("respfilter", sRespFiler);

    if (bAuthOK)
        p.modify("account", user);

    String sCond = " WHERE (status = 'Done')";

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
    } else {
        sCond += (sCond.length()>0 ? " AND " : "WHERE ");
        sCond += " (action = 'delete replica')";
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

    if (sReqFilter.length() > 0) {
        String q = IntervalQuery.stringMatching(sReqFilter, "source");

        if (q.length() > 0)
            sCond = IntervalQuery.cond(sCond, q);
    }

    if (sRespFiler.length() > 0) {
        String q = IntervalQuery.stringMatching(sRespFiler, "responsible");

        if (q.length() > 0)
            sCond = IntervalQuery.cond(sCond, q);
    }

    DB db = new DB();

    db.query("select count(1) as nr_rows from rawdata_runs_action rra left outer join rawdata_runs rr using(run)" + sCond);

    int iPage = rw.geti("p", 0);
    int iLimit = rw.geti("limit", 100);
    int iTotalCnt = db.geti("nr_rows", 0);
    String sLimit = "";

    if (iLimit > 0) {
        if (iPage < 0)
            iPage = 0;

        sLimit = " LIMIT " + iLimit + " OFFSET " + (iPage * iLimit);

        if (iTotalCnt > (iPage + 1) * iLimit) {
            p.comment("com_next", true);
            p.modify("next_page", iPage + 1);
        } else {
            p.comment("com_next", false);
        }

        if (iPage > 0) {
            p.comment("com_prev", true);
            p.modify("prev_page", iPage - 1);
        } else {
            p.comment("com_prev", false);
        }
    } else {
        p.comment("com_next", false);
        p.comment("com_prev", false);
    }

    p.modify("limit_" + iLimit, "selected");

    String select = "select run, action, filter, counter, rra.size, percentage, sourcese, source, responsible, status, addtime, log_message, partition from rawdata_runs_action rra left outer join rawdata_runs rr using(run)" + sCond + " order by addtime desc, partition desc" + sLimit;

    db.query(select);
    TreeSet<Integer> runList = new TreeSet<Integer>();

    int iRuns = 0;
    int iOldRun = 0;
    int iDeletedFiles = 0;
    long lTotalDeletedSize = 0;
    while (db.moveNext()) {
        final int iRun = db.geti("run");

        /*if (iRun == iOldRun)
            continue;*/
        runList.add(iRun);
        //iOldRun = iRun;

        pLine.modify("runpartition", db.gets("partition", ""));

        iDeletedFiles += db.geti("counter");
        lTotalDeletedSize += db.getl("size");
        pLine.fillFromDB(db);

        iRuns++;
        pLine.comment("com_authenticated", bAuthOK);
        p.append(pLine);
    }

    p.modify("all_runs", Format.toCommaList(runList));

    p.modify("runs", iRuns);
    p.modify("deleted_files", iDeletedFiles);
    p.modify("deleted_size", lTotalDeletedSize);

    pMaster.append(p);
    pMaster.write();

    String s = new String(baos.toByteArray());
    out.println(s);
%>