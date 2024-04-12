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
        if (!runInfos.isEmpty()) {
            RunInfo runInfo = runInfos.iterator().next();
            if (!runInfo.getRunQuality().equalsIgnoreCase(runQuality)) {
                logMessage("The run quality for run " + run
                        + "has been changed from " + runQuality + " to " + runInfo.getRunQuality());
                RunInfoUtils.fetchRunInfo(new HashSet<>(Arrays.asList(run)));
            } else
                isOk = true;
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
    String user = null;
    session.setAttribute("user_authenticated", "true");
    if (request.isSecure()) {
        X509Certificate cert[] = (X509Certificate[]) request.getAttribute("javax.servlet.request.X509Certificate");
        if (cert != null && cert.length > 0) {
            AlicePrincipal principal = new AlicePrincipal(cert[0].getSubjectDN().getName());
            user = principal.getName();
            if (user != null && user.length() > 0) {
                Set<String> sRoles = LDAPHelper.checkLdapInformation("users="+user, "ou=Roles,", "uid");
                bAuthOK = sRoles.contains("rawdatamgr");
                session.setAttribute("user_account", user);
            }
        }
    }

    if (!bAuthOK)
        return;

    final Set<Long> idsToDelete = new LinkedHashSet<>();
    final RequestWrapper rw = new RequestWrapper(request);
    final String sIds = rw.gets("ids");

    String q = IntervalQuery.numberInterval(sIds, "id");
    String[] rSplit = q.split("id=");
    for (int i = 0; i < rSplit.length; i++) {
        if (rSplit[i].length() > 0) {
            String[] orSplit = rSplit[i].split("OR");
            if (orSplit.length > 0)
                idsToDelete.add(Long.parseLong(orSplit[0].stripLeading().stripTrailing()));
            else
                idsToDelete.add(Long.parseLong(rSplit[i].stripLeading().stripTrailing()));
        }
    }

    final DB db = new DB();
    for (Long id : idsToDelete) {
        String select = "select * from rawdata_runs_action where (status = 'Queued' or status = 'Warning') and id_record = " + id + ";";
        if (db.query(select)) {
            String source = db.gets("source");
            Long run = db.getl("run");
            String action = db.gets("action");
            String filter = db.gets("filter");
            String log_message = db.gets("log_message");
            Integer counter = db.geti("counter");
            Long size = db.getl("size");
            String sourcese = db.gets("sourcese", null);
            String targetse = db.gets("targetse", null);
            Integer percentage = db.geti("percentage", 0);

            if (percentage == 0 || percentage == 100)
                percentage = null;

            if (source.equals("Deletion Thread")) {
                // check if run quality is still test and lastmodified >= one month;
                String query = "delete from rawdata_runs_action where id_record = " + id + ";";
                if (!checkRunForQuality(run, "Test") || !checkRunForLastModified(run)) {
                    db.query(query);
                    continue;
                }
            }

            Long id_record = RunActionUtils.insertRunAction(run, action, filter, source, log_message, counter, size,
                    sourcese, targetse, "In progress", percentage, user, id);

            if (id_record > 0)
                DeletionUtils.delTask(id_record, "rawdata_runs_action");
        }
    }

    lia.web.servlets.web.Utils.logRequest("/admin/deletion/delete.jsp?ids="+sIds, 1, request);
%>

<script type="text/javascript">
    window.history.back();
</script>