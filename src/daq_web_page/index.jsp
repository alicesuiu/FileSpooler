<%@ page import="lia.Monitor.Store.Fast.DB,lazyj.*,alimonitor.*,java.util.*,java.io.*,java.util.Date,java.text.SimpleDateFormat,lia.web.utils.ServletExtension,lia.Monitor.Store.Cache,java.security.cert.*,auth.*,utils.IntervalQuery, java.time.format.DateTimeFormatter, java.time.Instant, java.time.ZoneId" %><%
    if (!lia.web.utils.ThreadedPage.acceptRequest(request, response))
        return;

    lia.web.servlets.web.Utils.logRequest("START /DAQ/index.jsp", 0, request);

    // ----- init
    final RequestWrapper rw = new RequestWrapper(request);

    final ByteArrayOutputStream baos = new ByteArrayOutputStream(10000);

    String resPath = rw.gets("res_path", "");

    if (!resPath.endsWith("/"))
        resPath += "/";

    boolean dump = false;

    if (resPath.indexOf("xml") >= 0) {
        response.setContentType("text/xml");
        response.setHeader("Content-Disposition", "inline;filename=\"DAQ.xml\"");
    }
    else
    if (resPath.indexOf("mif") >= 0) {
        response.setContentType("text/mif");
        response.setHeader("Content-Disposition", "inline;filename=\"DAQ.mif\"");
    }
    else
    if (resPath.indexOf("csv") >= 0) {
        response.setContentType("text/csv");
        response.setHeader("Content-Disposition", "inline;filename=\"DAQ.csv\"");
    }
    else
    if (resPath.indexOf("json") >= 0) {
        response.setContentType("application/json");
        response.setHeader("Content-Disposition", "inline;filename=\"DAQ.json\"");
    }


    final Page pMaster = new Page(baos, "WEB-INF/res/" + resPath + "masterpage/masterpage.res");
    final Page p = new Page(resPath + "DAQ/index.res");
    final Page pLine = new Page(resPath + "DAQ/index_line.res");

    // ----- masterpage settings

    pMaster.comment("com_alternates", false);
    pMaster.modify("refresh_time", "300");
    pMaster.modify("title", "DAQ RAW Data Registration - Runs summary");


    // ----- parameters

    final int iHours = rw.geti("time", 24);
    final String sPartitionFilter = rw.gets("partitionfilter");
    final int iStagingFilter = rw.geti("staging", 0);
    final String sRunFilter = rw.gets("runfilter");
    final int iTransferFilter = rw.geti("transfer", 0);
    final int iProcessingFilter = rw.geti("processing", 0);
    final int iRunLength = rw.geti("runlength", 0);
    final int iGoodRun = rw.geti("goodrun", 0);
    final int iAction = rw.geti("raction", 0);
    final int iRunType = rw.geti("rtype", 0);
    final String sReplicaCtfFilter = rw.gets("replicaCtfFilter");
    final String sReplicaTfFilter = rw.gets("replicaTfFilter");
    final String sReplicaOtherFilter = rw.gets("replicaOtherFilter");

    final Set<Integer> sPreviouslyCheckedRuns = new HashSet<Integer>();
    for (String s: rw.getValues("pfo")) {
        sPreviouslyCheckedRuns.add(Integer.parseInt(s));
    }

    final Set<Integer> sCurrentlyCheckedRuns = new HashSet<Integer>();
    for (String s: rw.getValues("processing_flag")) {
        sCurrentlyCheckedRuns.add(Integer.parseInt(s));
    }

    final Set<Integer> sRemovedRuns = new HashSet<Integer>(sPreviouslyCheckedRuns);
    sRemovedRuns.removeAll(sCurrentlyCheckedRuns);

    final Set<Integer> sAddedRuns = new HashSet<Integer>(sCurrentlyCheckedRuns);
    sAddedRuns.removeAll(sPreviouslyCheckedRuns);

    // ------
    pMaster.modify("bookmark", "/DAQ/?time=" + iHours +
            (sPartitionFilter.length() > 0 ? "&partitionfilter=" + Format.encode(sPartitionFilter) : "") +
            (iStagingFilter > 0 ? "&staging=" + iStagingFilter : "") +
            (sRunFilter.length() > 0 ? "&runfilter=" + Format.encode(sRunFilter) : "") +
            (iTransferFilter > 0 ? "&transfer=" + iTransferFilter : "") +
            (iProcessingFilter > 0 ? "&processing=" + iProcessingFilter : "") +
            (iRunLength > 0 ? "&runlength=" + iRunLength : "") +
            (iGoodRun > 0 ? "&goodrun=" + iGoodRun : "") +
            (iAction > 0 ? "&raction=" + iAction : "") +
            (iRunType > 0 ? "&rtype=" + iRunType : "") +
            (sReplicaCtfFilter.length() > 0 ? "&replicaCtfFilter=" + Format.encode(sReplicaCtfFilter) : "") +
            (sReplicaTfFilter.length() > 0 ? "&replicaTfFilter=" + Format.encode(sReplicaTfFilter) : "") +
            (sReplicaOtherFilter.length() > 0 ? "&replicaOtherFilter=" + Format.encode(sReplicaOtherFilter) : "")
    );

    // ----- See if we switched to admin mode and we have to enable some run
    boolean bAuthOK = false;

    if (request.isSecure()){
        X509Certificate cert[] = (X509Certificate[]) request.getAttribute("javax.servlet.request.X509Certificate");

        if (cert!=null && cert.length>0){
            AlicePrincipal principal = new AlicePrincipal(cert[0].getSubjectDN().getName());

            String sName = principal.getName();

            if (sName!=null && sName.length()>0){
                Set<String> sRoles = LDAPHelper.checkLdapInformation("users="+sName, "ou=Roles,", "uid");

                bAuthOK = sRoles.contains("webadmin");
            }
        }
    }

    if (bAuthOK){
        int iRun = rw.geti("run");

        DB db = new DB();

        if (iRun>0){
            String sQuery = "UPDATE rawdata_runs SET staging_status=1 WHERE run='"+iRun+"' AND staging_status=0;";

            db.syncUpdateQuery(sQuery);
        }

        int iDel = rw.geti("delete");

        if (iDel>0){
            String sQuery = "UPDATE rawdata_runs SET staging_status=0 WHERE run='"+iDel+"' AND staging_status=3;";

            db.syncUpdateQuery(sQuery);
        }

	/*
	for (Integer iAddedRun : sAddedRuns)
	    db.syncUpdateQuery("UPDATE rawdata_runs SET processing_flag=1 WHERE run='"+iAddedRun+"' AND processing_flag=0;");

	for (Integer iRemovedRun : sRemovedRuns)
	    db.syncUpdateQuery("UPDATE rawdata_runs SET processing_flag=0 WHERE run='"+iRemovedRun+"' AND processing_flag>0;");

	if (sAddedRuns.size()>0){
	    try{
	        Process child = lia.util.MLProcess.exec(new String[]{"/home/monalisa/MLrepository/bin/processing/processing_bg.sh"});
		child.waitFor();
	    }
	    catch (Throwable t){
		System.err.println("DAQ/index.jsp : "+t+" ("+t.getMessage()+")");
		t.printStackTrace();
	    }
	}
	*/
    }

    // ----- page contents

    p.modify("time_"+iHours, "selected");
    p.modify("staging_"+iStagingFilter, "selected");
    p.modify("not_secure", bAuthOK ? "false" : "true");
    p.modify("runfilter", sRunFilter);
    p.modify("partitionfilter", sPartitionFilter);
    p.modify("transfer_"+iTransferFilter, "selected");
    p.modify("processing_"+iProcessingFilter, "selected");
    p.modify("runlength_"+iRunLength, "selected");
    p.modify("goodrun_"+iGoodRun, "selected");
    p.modify("action_"+iAction, "selected");
    p.modify("rtype_"+iRunType, "selected");
    p.modify("replicaCtfFilter", sReplicaCtfFilter);
    p.modify("replicaTfFilter", sReplicaTfFilter);
    p.modify("replicaOtherFilter", sReplicaOtherFilter);


    String sCond = "";
    boolean bIntervalFilter = true;

    if (sPartitionFilter.length() > 0) {
        String q = IntervalQuery.stringMatching(sPartitionFilter, "partition");

        if (q.length() > 0)
            sCond = IntervalQuery.cond(sCond, q);

        bIntervalFilter = false;
    }

    if (sReplicaCtfFilter.length() > 0) {
        String q = IntervalQuery.stringMatching(sReplicaCtfFilter, "ctf");

        if (q.length() > 0)
            sCond = IntervalQuery.cond(sCond, q) + " AND (ctf_file_count > 0) ";

        bIntervalFilter = false;
    }

    if (sReplicaTfFilter.length() > 0) {
        String q = IntervalQuery.stringMatching(sReplicaTfFilter, "tf");

        if (q.length() > 0)
            sCond = IntervalQuery.cond(sCond, q) + " AND (tf_file_count > 0) ";

        bIntervalFilter = false;
    }

    if (sReplicaOtherFilter.length() > 0) {
        String q = IntervalQuery.stringMatching(sReplicaOtherFilter, "other");

        if (q.length() > 0)
            sCond = IntervalQuery.cond(sCond, q) + " AND (other_file_count > 0) ";

        bIntervalFilter = false;
    }


    if (bIntervalFilter == true)
        sCond = iHours <= 0 ? "" : "WHERE mintime>extract(epoch from now()-'"+iHours+" hours'::interval)::int";

    if (iStagingFilter > 0) {
        sCond += sCond.length() > 0 ? " AND " : "WHERE ";

        switch (iStagingFilter) {
            case 3:
                sCond += "staging_status=0";
                break;
            case 1:
                sCond += "staging_status=3";
                break;
            case 2:
                sCond += "(staging_status=1 OR staging_status=2)";
                break;
        }
    }

    if (sRunFilter.length() > 0) {
        String q = IntervalQuery.numberInterval(sRunFilter, "rrd.run");

        if (q.length() > 0)
            sCond = IntervalQuery.cond(sCond, q);
    }

    if (iTransferFilter > 0) {
        if (iTransferFilter > 1)
            sCond += (sCond.length() > 0 ? " AND " : "WHERE ") + "transfer_status=" + (iTransferFilter - 1);
        else
            sCond += (sCond.length() > 0 ? " AND " : "WHERE ") + "(transfer_status=0 or transfer_status is null)";
    }

    if (iProcessingFilter>0) {
        sCond += (sCond.length()>0 ? " AND " : "WHERE ");

        if (iProcessingFilter==1)
            sCond += " rrd.run in (select run from rawdata_processing_requests where status=1)";
        else if (iProcessingFilter==2)
            sCond += "(rrd.run in (select run from rawdata_processing_requests where status=2) AND processed_chunks>0)";
        else
            sCond += " (rrd.run not in (select run from rawdata_processing_requests))";
    }

    if (iRunLength > 0) {
        sCond += (sCond.length() > 0 ? " AND " : "WHERE ");

        if (iRunLength == 1) {
            sCond += " (duration < 300000)";
        }
        else {
            sCond += " (duration >= 300000)";
        }
    }

    if (iGoodRun > 0){
        sCond += (sCond.length() > 0 ? " AND " : "WHERE ");

        if (iGoodRun == 1) {
            sCond += " (daq_goodflag=1)";
        }
        else if (iGoodRun == 2) {
            sCond += " (daq_goodflag=0)";
        }
        else if (iGoodRun == 4) {
            sCond += " (daq_goodflag=2)";
        }
        else{
            sCond += " (daq_goodflag is null)";
        }
    }


    if (iAction > 0 ) {
        sCond += (sCond.length()>0 ? " AND " : "WHERE ");

        if (iAction == 1)
            sCond += " rrd.run in (select run from rawdata_runs_action where action='copy' and status = 'Done')";
        else if (iAction == 2)
            sCond += " rrd.run in (select run from rawdata_runs_action where action='move' and status = 'Done')";
        else if (iAction == 3)
            sCond += " rrd.run in (select run from rawdata_runs_action where action='delete' and status = 'Done')";
        else if (iAction == 4)
            sCond += " rrd.run not in (select distinct run from rawdata_runs_action where (action = 'copy' OR action = 'move' OR action = 'delete' OR action = 'delete replica') and status = 'Done')";
        else if (iAction == 5)
            sCond += " rrd.run in (select run from rawdata_runs_action where action='delete replica' and status = 'Done')";
        else if (iAction == 6)
            sCond += " rrd.run not in (select distinct run from rawdata_runs_action where action = 'delete' and status = 'Done' and filter = 'all')";
    }

    if (iRunType > 0) {
        sCond += (sCond.length()>0 ? " AND " : "WHERE ");

        if (iRunType == 1)
            sCond += " (file_types like '%raw%')";
        else if (iRunType == 2)
            sCond += " (file_types like '%calib%')";
        else if (iRunType == 3)
            sCond += " (file_types like '%other%')";
        else if (iRunType == 4)
            sCond += " (file_types is null)";
    }

    //sCond += sCond.length()>0 ? " AND " : " WHERE ";
    //sCond += "(pass IS NULL or pass=1)";

    String sQuery = "select *,size/chunks as avg_file_size from rawdata_runs_details rrd left outer join rawdata_runs_last_action rla using(run) "+sCond+" order by maxtime desc,rrd.run,pass!=1,pass!=0 desc,targetse,status desc;";

    //System.err.println(sQuery);

    DB db = new DB(sQuery);

    int iRuns = 0;
    int iFiles = 0;
    long lTotalCTFFiles = 0;
    long lTotalTFiles = 0;
    long lTotalOtherFiles = 0;
    long lTotalSize = 0;
    long lTotalCTFSize = 0;
    long lTotalTFSize = 0;
    long lTotalOtherSize = 0;
    long lTotalTestRuns = 0;
    long lTotalGoodRuns = 0;
    long lTotalBadRuns = 0;
    long lTotalGoodRunsDuration = 0;
    long lTotalBadRunsDuration = 0;
    long lTotalTestRunsDuration = 0;
    long lTotalRunsDuration = 0;

    int iTransfersCompleted = 0;
    int iTransfersScheduled = 0;
    int iTransfersUnknown   = 0;
    int iTransfersFailed    = 0;

    int iJobsCompleted = 0;
    int iJobsStarted   = 0;
    int iJobsUnknown   = 0;

    int iStagingCompleted = 0;
    int iStagingStarted   = 0;
    int iStagingUnknown   = 0;

    int iOldRun = 0;

    TreeSet<Integer> runList = new TreeSet<Integer>();

    while (db.moveNext()) {
        final int iRun = db.geti("run");

        if (iRun == iOldRun)
            continue;

        runList.add(iRun);

        iOldRun = iRun;

        pLine.fillFromDB(db);
        pLine.comment("com_auth", bAuthOK);

        iRuns++;

        pLine.modify("counter", iRuns);

        long size = db.getl("size");
        int chunks = db.geti("chunks");

        iFiles += chunks;
        lTotalSize += size;

        if (chunks>0){
            pLine.modify("avg_file_size", size / chunks);
        }

        int iTransferStatus = db.geti("transfer_status", -1);

        switch (iTransferStatus) {
            case -1:
            case 0:
                iTransfersUnknown ++;
                break;
            case 1:
                pLine.modify("transferstatus_bgcolor", "#FFFF00");
                iTransfersScheduled ++;
                break;
            case 2:
                pLine.modify("transferstatus_bgcolor", "#00FF00");
                iTransfersCompleted ++;
                break;
            case 3:
                pLine.modify("transferstatus_bgcolor", "#FF0000");
                iTransfersFailed ++;
                break;
            default:
                iTransfersUnknown++;
        }

        int iProcessing = db.geti("status");

        if (iProcessing==1){
            pLine.modify("processingstatus_bgcolor", "#FFFF00");
            iJobsStarted++;
        }
        else if (iProcessing==2 || db.geti("processed_chunks")>0) {
            pLine.modify("processingstatus_bgcolor", "#00FF00");
            iJobsCompleted++;
        } else {
            iJobsUnknown++;
        }

        int iStagingStatus = db.geti("staging_status");

        switch (iStagingStatus) {
            case  3: iStagingCompleted++; break;
            case  2:
            case  1: iStagingStarted++; break;
            default: iStagingUnknown++;
        }

        if (iStagingStatus>0) {
            pLine.modify("stagingstatus_bgcolor", iStagingStatus==3 ? "#00FF00" : "#FFFF00");
        }

        pLine.comment("com_remove", bAuthOK && iStagingStatus==3);
        pLine.comment("com_order", bAuthOK && iStagingStatus==0);

        String sPath = db.gets("collection_path");

        if (sPath.indexOf("/")>=0) {
            sPath = sPath.substring(0, sPath.lastIndexOf("/")+1) + "ESDs";
            pLine.modify("esds_path", sPath);
        }

        pLine.comment("com_processing_flag", iProcessing>0 || db.geti("processed_chunks")>0);
        pLine.comment("com_processing_flag_auth", iProcessing==1 && bAuthOK);

        pLine.comment("com_errorv", db.geti("errorv_count")>0);

        pLine.comment("com_job", db.getl("req_masterjob_id")>0);

        pLine.modify("runlength", db.getl("duration")/1000);
        lTotalRunsDuration += db.getl("duration");

        switch (db.geti("daq_goodflag", -1)){
            case -1: pLine.modify("goodrun", "n/a"); break;
            case 0:  pLine.modify("goodrun", "Bad");
                lTotalBadRuns++;
                lTotalBadRunsDuration += db.getl("duration");
                break;
            case 1:  pLine.modify("goodrun", "OK");
                lTotalGoodRuns++;
                lTotalGoodRunsDuration += db.getl("duration");
                break;
            case 2:  pLine.modify("goodrun", "Test");
                lTotalTestRuns++;
                lTotalTestRunsDuration += db.getl("duration");
                break;
        }

        if (db.geti("tf_file_count") > 0) {
            pLine.modify("xtf_file_count", db.geti("tf_file_count"));
            lTotalTFiles += db.geti("tf_file_count");
        }

        if (db.geti("ctf_file_count") > 0) {
            pLine.modify("xctf_file_count", db.geti("ctf_file_count"));
            lTotalCTFFiles += db.geti("ctf_file_count");
        }

        if (db.geti("other_file_count") > 0) {
            pLine.modify("xother_file_count", db.geti("other_file_count"));
            lTotalOtherFiles += db.geti("other_file_count");
        }

        if (db.getl("tf_file_size") > 0) {
            pLine.modify("xtf_file_size", db.getl("tf_file_size"));
            lTotalTFSize += db.getl("tf_file_size");
        }

        if (db.getl("ctf_file_size") > 0) {
            pLine.modify("xctf_file_size", db.getl("ctf_file_size"));
            lTotalCTFSize += db.getl("ctf_file_size");
        }

        if (db.getl("other_file_size") > 0) {
            pLine.modify("xother_file_size", db.getl("other_file_size"));
            lTotalOtherSize += db.getl("other_file_size");
        }

        pLine.modify("global_run_type", db.gets("file_types"));

        DB db1 = new DB();
        String action = null, selectRunAction, log_message = "";
        long actionTime = 0;

        if (iAction == 1)
            selectRunAction = "select action, addtime, filter, log_message, status from rawdata_runs_action where run=" + iRun + " and action='copy' and status = 'Done' order by addtime desc limit 1;";
        else if (iAction == 2)
            selectRunAction = "select action, addtime, filter, log_message, status from rawdata_runs_action where run=" + iRun + " and action='move' and status = 'Done' order by addtime desc limit 1;";
        else if (iAction == 3)
            selectRunAction = "select action, addtime, filter, log_message, status from rawdata_runs_action where run=" + iRun + " and action='delete' and status = 'Done' order by addtime desc limit 1;";
        else if (iAction == 5)
            selectRunAction = "select action, addtime, filter, sourcese, log_message, status from rawdata_runs_action where run=" + iRun + " and action='delete replica' and status = 'Done' order by addtime desc limit 1;";
        else
            selectRunAction = "select action, addtime, filter, log_message, status from rawdata_runs_action where run=" + iRun + " and action != 'change run quality' and status = 'Done' order by addtime desc limit 1;";

        db1.query(selectRunAction);
        actionTime = db1.getl("addtime");
        if (actionTime > 0) {
            String actionFilter = "", sourcese = "";
            if (!db1.gets("filter").equalsIgnoreCase("all"))
                actionFilter += " " + db1.gets("filter");
            if (db1.gets("action").equalsIgnoreCase("delete replica"))
                sourcese += " " + db1.gets("sourcese");
            if (db1.gets("log_message").contains("skimmed"))
                log_message = "skimmed and ";
            action = log_message + db1.gets("action") + actionFilter + sourcese + " on " + Instant.ofEpochSecond(actionTime)
                    .atZone(ZoneId.of("Europe/Zurich")).format(DateTimeFormatter.ofPattern("dd.MM.yyyy"));
            pLine.modify("run_action", action);
        }

        if (db.geti("tf_file_count", 0) > 0)
            pLine.modify("tf_replica", db.gets("tf"));

        if (db.geti("ctf_file_count", 0) > 0)
            pLine.modify("ctf_replica", db.gets("ctf"));

        if (db.geti("other_file_count", 0) > 0)
            pLine.modify("other_replica", db.gets("other"));

        p.append(pLine);
    }

    p.modify("all_runs", Format.toCommaList(runList));

    p.modify("runs", iRuns);
    p.modify("files", iFiles);
    p.modify("totalsize", lTotalSize);
    p.modify("total_ctf_size", lTotalCTFSize);
    p.modify("total_tf_size", lTotalTFSize);
    p.modify("total_other_size", lTotalOtherSize);
    p.modify("total_ctf_count", lTotalCTFFiles);
    p.modify("total_tf_count", lTotalTFiles);
    p.modify("total_other_count", lTotalOtherFiles);

    if (iFiles>0) {
        p.modify("average_file_size", lTotalSize / iFiles);
    }

    p.modify("transfers_unknown", iTransfersUnknown);
    p.modify("transfers_scheduled", iTransfersScheduled);
    p.modify("transfers_completed", iTransfersCompleted);

    p.modify("jobs_unknown", iJobsUnknown);
    p.modify("jobs_started", iJobsStarted);
    p.modify("jobs_completed", iJobsCompleted);

    p.modify("staged_unknown", iStagingUnknown);
    p.modify("staged_started", iStagingStarted);
    p.modify("staged_completed", iStagingCompleted);

    p.modify("runs_duration", lTotalRunsDuration / 1000);
    p.modify("good_runs_duration", lTotalGoodRunsDuration / 1000);
    p.modify("bad_runs_duration", lTotalBadRunsDuration / 1000);
    p.modify("test_runs_duration", lTotalTestRunsDuration / 1000);

    p.modify("good_runs", lTotalGoodRuns);
    p.modify("bad_runs", lTotalBadRuns);
    p.modify("test_runs", lTotalTestRuns);

    /*String sPartQuery = "SELECT p FROM (SELECT distinct partition as p FROM rawdata_runs ";

    if (iHours>0)
	sPartQuery += "WHERE mintime>extract(epoch from now()-'"+iHours+" hours'::interval)::int";

    sPartQuery+=") AS x ORDER BY split_part(p,'_',1) DESC, p ASC";

    db.query(sPartQuery);

    while (db.moveNext()){
	String sPartition = db.gets(1);

	p.append("opt_partitions", "<option value='"+Format.escHtml(sPartition)+"' "+(sPartition.equals(sPartitionFilter)?"selected":"")+">"+Format.escHtml(sPartition)+"</option>");
    }*/

    // ----- closing

    pMaster.append(p);
    pMaster.write();

    String s = new String(baos.toByteArray());
    out.println(s);

    lia.web.servlets.web.Utils.logRequest("/DAQ/index.jsp", baos.size(), request);
%>
