<%@ page import="lazyj.*,lia.Monitor.Store.Fast.DB,utils.IntervalQuery,java.util.*,alien.policymaker.*,auth.*,java.security.cert.*,alien.catalogue.LFN" %><%
    lia.web.servlets.web.Utils.logRequest("START /admin/deletion/add.jsp", 0, request);

    final RequestWrapper rw = new RequestWrapper(request);

    final String sRuns = rw.gets("runs");
    String sFilter = rw.gets("filter").toLowerCase();
    String sStorage = rw.gets("se");
    Integer iLimit = rw.geti("limit");


    if (sRuns.length() == 0) {
%>
<form action=add.jsp method=post>
    <table border=0 cellspacing=10 cellpadding=0>

        <tr>
            <td>Runs:</td>
            <td><input type=text name=runs value="" class=input_text></td>
        </tr>

        <tr>
            <td>Storage:</td>
            <td><select name=se class=input_select>
                <%
                    String s = Format.escHtml("");
                    out.println("<option value='"+s+"'>"+s+"</option>");
                    final DB db = new DB("select se_name from list_ses ;");
                    while (db.moveNext()){
                        s = Format.escHtml(db.gets(1));
                        out.println("<option value='"+s+"'>"+s+"</option>");
                    }
                    s = Format.escHtml("ALL");
                    out.println("<option value='"+s+"'>"+s+"</option>");
                %>
            </select></td>
        </tr>

        <tr>
            <td>Filter:</td>
            <td><select name=filter class=input_select>
                <%
                    s = Format.escHtml("ALL");
                    out.println("<option value='"+s+"'>"+s+"</option>");
                    s = Format.escHtml("TF");
                    out.println("<option value='"+s+"'>"+s+"</option>");
                    s = Format.escHtml("CTF");
                    out.println("<option value='"+s+"'>"+s+"</option>");
                %>
            </select></td>
        </tr>

        <!-- <tr>
            <td>Limit:</td>
            <td><input type=number name=limit min=0 max=2147483647></td>
        </tr> -->

        <tr>
            <td>&nbsp;</td>
            <td><input type=submit value="Check request..." class=input_text>
        </tr>
    </table>
</form>
<%
        return;
    }

    String extension = null;
    if (sFilter.equals("tf"))
        extension = ".tf";
    else if (sFilter.equals("ctf"))
        extension = ".root";

    String action = "delete replica";
    if (sStorage.equals("all")) {
        sStorage = null;
        action = "delete";
    }

    if (iLimit <= 0)
        iLimit = null;

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

    if (sStorage.length() <= 0) {
        out.println("You have select a storage from which the runs will be deleted OR you can select ALL to delete the runs from all the storages!");
        return;
    }

    String user = null;
    if (request.isSecure()) {
        X509Certificate cert[] = (X509Certificate[]) request.getAttribute("javax.servlet.request.X509Certificate");
        if (cert!=null && cert.length>0) {
            AlicePrincipal principal = new AlicePrincipal(cert[0].getSubjectDN().getName());
            user = principal.getName();
        }
    }

    for (Long run : arrL) {
        Set<LFN> lfns = DeletionUtils.getLFNsForDeletion(run, null, extension, sStorage, null);
        if (lfns == null || lfns.isEmpty()) {
            out.println("Run " + run + " was already deleted!");
        } else {
            RunActionUtils.insertRunAction(run, action, sFilter, user,
                    "todo", lfns.size(), lfns.stream().mapToLong(lfn -> lfn.size).sum(),
                    sStorage, null, "Queued");
            out.println("The deletion of run " + run + " was queued.");
        }
    }

    /*String result = DeletionUtils.printMessages(arrL, false, true, true, sFilter, sStorage, iLimit);
	result = result.replaceAll("\n", "<BR>");
	out.println(result);*/

    /*Iterator<Long> runsIterator = arrL.iterator();
    while(runsIterator.hasNext())
    	out.println(runsIterator.next());
    out.println(sFilter + " " + sStorage + " " + iLimit);*/

   /*	Map<String, Set<String>> messages = DeletionUtils.getMessages();

    if (messages != null && !messages.isEmpty()) {
	    Set<String> infoLevel = DeletionUtils.getMessages().get("Info");
	    Set<String> warningLevel = DeletionUtils.getMessages().get("Warning");

	 	out.println(" ===== WARNING LEVEL! ===== " + " <BR>");
	 	Iterator<String> iter = warningLevel.iterator();
    	while(iter.hasNext()) {
    		String m = iter.next();
	    	if (m.contains("is different than the one in the LFNs list"))
	    		continue;
	    	out.println(m + "<BR>");
	    }

	    out.println(" ===== INFO LEVEL! ===== " + "<BR>");
	    iter = infoLevel.iterator();
	    while(iter.hasNext()) {
	    	String m = iter.next();
	    	if (m.contains("Successful deletion of"))
	    		out.println(m + "<BR>");
	    }
	}*/

    lia.web.servlets.web.Utils.logRequest("/admin/deletion/add.jsp?runs="+sRuns, 1, request);
%>