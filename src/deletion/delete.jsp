<%@ page import="auth.*,java.security.cert.*,java.util.*,lazyj.*,lia.Monitor.Store.Fast.DB,utils.IntervalQuery,alien.policymaker.*,java.time.*,java.io.*"%>
<%!
    private static PrintWriter pwLog = null;
    private static final synchronized void logMessage(final String message){
        final Date d = new Date();

        if (pwLog==null){
            try{
                pwLog = new PrintWriter(new FileWriter("/home/monalisa/MLrepository/logs/deleteRunsWebPage.log", true));
            }
            catch (Exception e){
                // ignore
            }
        }

        boolean logged = false;

        if (pwLog!=null){
            try{
                pwLog.println(d+": "+message);
                pwLog.flush();

                logged = true;
            }
            catch (Exception e){
                pwLog = null;
            }
        }

        if (!logged)
            System.err.println("/admin/deletion/delete.jsp: "+d+" : "+message);
    }

    private static boolean checkRunForQuality(Long run, String runQuality) {
        Set<RunInfo> runInfos = RunInfoUtils.getRunInfoFromLogBook(String.valueOf(run));
        boolean isOk = false;
        try {
            if (!runInfos.isEmpty()) {
                RunInfo runInfo = runInfos.iterator().next();
                if (!runInfo.getRunQuality().equalsIgnoreCase(runQuality)) {
                    logMessage("The run quality for run " + run
                            + "has been changed from " + runQuality + " to " + runInfo.getRunQuality());
                    RunInfoUtils.fetchRunInfo(new HashSet<>(Arrays.asList(run)));
                } else
                    isOk = true;
            }
        } catch (HandleException he) {
            //he.sendMail();
        }
        return isOk;
    }

    private static boolean checkRunForLastModified(Long run) {
        long lastmodified = ZonedDateTime.now(ZoneId.of("Europe/Zurich"))
                .minusWeeks(4).toInstant().toEpochMilli() / 1000;
        DB db = new DB();
        boolean isOk = false;
        if (db.query("select lastmodified from rawdata_runs where run = " + run + ";")) {
            Long lastmodifiedDB = db.getl("lastmodified");
            if (lastmodifiedDB > lastmodified) {
                logMessage("Run " + run + "was recently updated!");
            } else
                isOk = true;
        }
        return isOk;
    }
%>

<%
    lia.web.servlets.web.Utils.logRequest("START /admin/deletion/delete.jsp", 0, request);
    boolean bAuthOK = false;
    session.setAttribute("user_authenticated", "true");
    if (request.isSecure()) {
        X509Certificate cert[] = (X509Certificate[]) request.getAttribute("javax.servlet.request.X509Certificate");
        if (cert != null && cert.length > 0) {
            AlicePrincipal principal = new AlicePrincipal(cert[0].getSubjectDN().getName());
            String sName = principal.getName();
            if (sName != null && sName.length() > 0) {
                Set<String> sRoles = LDAPHelper.checkLdapInformation("users="+sName, "ou=Roles,", "uid");
                bAuthOK = sRoles.contains("rawdatamgr");
                session.setAttribute("user_account", sName);
            }
        }
    }

    if (!bAuthOK)
        return;

    final Set<Long> runsToDelete = new LinkedHashSet<>();
    final RequestWrapper rw = new RequestWrapper(request);
    final String sRuns = rw.gets("runs");

    String q = IntervalQuery.numberInterval(sRuns, "run");
    String[] rSplit = q.split("run=");
    for (int i = 0; i < rSplit.length; i++) {
        if (rSplit[i].length() > 0) {
            String[] orSplit = rSplit[i].split("OR");
            if (orSplit.length > 0)
                runsToDelete.add(Long.parseLong(orSplit[0].stripLeading().stripTrailing()));
            else
                runsToDelete.add(Long.parseLong(rSplit[i].stripLeading().stripTrailing()));
        }
    }

    out.println("Runs for deletion: " + sRuns + "<br><br>");

    final DB db = new DB();
    for (Long run : runsToDelete) {
        String select = "select * from rawdata_runs_action where status = 'Queued' and run = " + run + ";";
        if (db.query(select)) {
            String source = db.gets("source");
            if (source.equals("Deletion Thread")) {
                // check if run quality is still test and lastmodified >= one month;
                if (!checkRunForQuality(run, "Test"))
                    continue;
                if (!checkRunForLastModified(run))
                    continue;
            }

            String extension = null;
            String filter = db.gets("filter");
            if (filter.equals("tf"))
                extension = ".tf";
            else if (filter.equals("ctf"))
                extension = ".root";

            String storage = db.gets("sourcese");
            Integer percentage = db.geti("percentage", 0);

            String update = "update rawdata_runs_action set status = 'In progress' where run = " + run + " and status = 'Queued';";
            if (!db.syncUpdateQuery(update)) {
                logMessage("The update action for run " + run + " failed");
                return;
            }
            logMessage("Update raw for run : " + run + " in rawdata_runs_action");

            DeletionUtils.deleteRuns(new HashSet<>(Arrays.asList(run)), null, extension, storage, percentage);
            out.println("Run " + run + "was successfully deleted!<br>");
        }
    }

    lia.web.servlets.web.Utils.logRequest("/admin/deletion/delete.jsp?runs="+sRuns, 1, request);
%>