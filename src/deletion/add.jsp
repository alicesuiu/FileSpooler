<%@ page import="lazyj.*,lia.Monitor.Store.Fast.DB,utils.IntervalQuery,java.util.*,alien.policymaker.*,auth.*,java.security.cert.*,alien.catalogue.LFN,java.util.concurrent.ExecutorService,java.util.concurrent.Executors,java.util.concurrent.Future,alien.se.SE,alien.se.SEUtils" %><%
    lia.web.servlets.web.Utils.logRequest("START /admin/deletion/add.jsp", 0, request);

    boolean bAuthOK = false;
    String user = null;
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

    if (!bAuthOK)
        return;

    final RequestWrapper rw = new RequestWrapper(request);
    final String sRuns = rw.gets("runs");
    String[] filterValues = request.getParameterValues("filter");
    Integer iLimit = rw.geti("limit");
    String sReason = rw.gets("reasonBox");
    String[] storageValues = request.getParameterValues("se");
    String hiddenButton = rw.gets("hiddenButton");

    String q = IntervalQuery.numberInterval(sRuns, "run");
    String[] rSplit = q.split("run=");
    Set<Long> arrL = new HashSet<>();
    for (int i = 0; i < rSplit.length; i++) {
        if (rSplit[i].length() > 0) {
            String[] orSplit = rSplit[i].split("OR");
            if (orSplit.length > 0)
                arrL.add(Long.parseLong(orSplit[0].stripLeading().stripTrailing()));
            else
                arrL.add(Long.parseLong(rSplit[i].stripLeading().stripTrailing()));
        }
    }

    Set<String> defaultStorages = Collections.synchronizedSet(new HashSet<>());
    ExecutorService executor = Executors.newFixedThreadPool(4);
    Set<Future> objects = new HashSet<>();
    Set<LFN> allLFNs = new HashSet<>();

    for (Long run : arrL) {
        Set<LFN> lfns = RunInfoUtils.getLFNsFromRawdataDetails(run, "all");
        allLFNs.addAll(lfns);
    }

    for (LFN lfn : allLFNs)
        objects.add(executor.submit(new GetReplicasTask(defaultStorages, lfn)));

    try {
        for (Future future : objects)
            future.get();
    } catch (Exception e) {
        // todo
    }
    executor.shutdown();

    if (sReason.isBlank() || storageValues == null || storageValues.length <= 0 || filterValues == null || filterValues.length <= 0) {
%>
<form action=add.jsp method=post>
    <table border=0 cellspacing=10 cellpadding=0>

        <tr>
            <td>Runs:</td>
            <td><input type=text name=runs value="<%= sRuns %>" class=input_text readonly style="background-color: #f2f2f2; color: #666; border: 1px solid #ddd; padding: 5px;"></td>
        </tr>

        <tr>
            <td>Storage filter*:</td>
            <td><select name=se class=input_select multiple>
                <%
                    String s = Format.escHtml("ALL");
                    out.println("<option value='"+s+"'>"+s+"</option>");
                    for (String storage : defaultStorages) {
                        SE se = SEUtils.getSE(storage);
                        if (se.isQosType("disk") || se.isQosType("special"))
                            s = Format.escHtml(storage) + Format.escHtml(" (Disk)");
                        else if (se.isQosType("tape"))
                            s = Format.escHtml(storage) + Format.escHtml(" (Tape)");
                        out.println("<option value='"+s+"'>"+s+"</option>");
                    }
                %>
            </select></td>
        </tr>

        <tr>
            <td>Data filter*:</td>
            <td><select name=filter class=input_select multiple>
                <%
                    s = Format.escHtml("ALL");
                    out.println("<option value='"+s+"'>"+s+"</option>");
                    s = Format.escHtml("TF");
                    out.println("<option value='"+s+"'>"+s+"</option>");
                    s = Format.escHtml("CTF");
                    out.println("<option value='"+s+"'>"+s+"</option>");
                    s = Format.escHtml("Other");
                    out.println("<option value='"+s+"'>"+s+"</option>");
                %>
            </select></td>
        </tr>

        <tr>
            <td>Percentage of files to delete:</td>
            <td><input type=number name=limit min=1 max=100 value="100"></td>
        </tr>

        <tr>
            <td>Reason for deletion*:</td>
            <td><textarea name="reasonBox" rows="10" cols="50"></textarea></td>
        </tr>

        <tr>
            <td>&nbsp;</td>
            <td><input type=submit value="Add request..." class=input_text onClick="setHiddenButton()"></td>
        </tr>
    </table>

    <input type="hidden" id="hiddenButton" name="hiddenButton" value="false">
</form>

<script>
    function setHiddenButton() {
        var hiddenButton = document.getElementById("hiddenButton");
        if (hiddenButton.value === "false") {
            hiddenButton.value = "true";
        }
    }
</script>
<%
        out.println("[Warning]<br>");
        out.println("Through this form you will only add a pending request to delete data!<br>");
        out.println("To really delete data you will have to tick the desired boxes under the 'Really delete data' column on the next page!<br>");
        if (hiddenButton.equalsIgnoreCase("true")) {
            if (sReason.isBlank())
                out.println("You have to provide a reason why you want to delete the selected run/runs!<br>");

            if (storageValues == null || storageValues.length <= 0)
                out.println("You have to select at least one storage!<br>");

            if (filterValues == null || filterValues.length <= 0)
                out.println("You have to select at least one filter!<br>");

        }
        out.println("<br>[Info]<br>");
        out.println("All added requests are logged!!<br>");
        return;
    }

    Map<String, List<String>> seMap = RunInfoUtils.getSEs();
    String action = "delete replica";
    ArrayList<String> storageValuesList = new ArrayList<>(Arrays.asList(storageValues));
    ArrayList<String> filterValuesList = new ArrayList<>(Arrays.asList(filterValues));

    if (storageValuesList.contains("ALL")) {
        action = "delete";
        storageValuesList.remove("ALL");
    }
    storageValuesList.replaceAll(s -> s.replaceAll(" \\(Disk\\)| \\(Tape\\)", "").stripTrailing());

    if (filterValuesList.contains("ALL"))
        filterValuesList.removeIf(f -> !f.equalsIgnoreCase("all"));

    if (iLimit <= 0 || iLimit >= 100)
        iLimit = null;

    String sourcese = String.join(" ", storageValuesList);
    if (action.equalsIgnoreCase("delete"))
        sourcese = null;

    for (Long run : arrL) {
        for (String sFilter : filterValuesList) {
            if (!DeletionUtils.hasDuplicates(run, action, sFilter.toLowerCase(), sourcese, iLimit)) {
                Long id_record = RunActionUtils.insertRunAction(run, action, sFilter.toLowerCase(), user, sReason, 0, 0L,
                        sourcese, null, "Inserting", iLimit, "todo", null);
                if (id_record > 0)
                    DeletionUtils.addTask(id_record, "rawdata_runs_action");
            }
        }
    }

    lia.web.servlets.web.Utils.logRequest("/admin/deletion/add.jsp?runs="+sRuns+"&datafilter="+String.join(" ", filterValuesList)+"&reason="+sReason+"&percentage="+iLimit+"&ses="+sourcese, 1, request);
%>
