<%@ page import="auth.*,java.security.cert.*,java.util.*,lazyj.*,lia.Monitor.Store.Fast.DB,utils.IntervalQuery" %><%
    lia.web.servlets.web.Utils.logRequest("START /deletion/del_request.jsp", 0, request);

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

    if (!bAuthOK)
        return;

    final RequestWrapper rw = new RequestWrapper(request);
    final Long lRequest = rw.getl("id");
    String delete = "delete from rawdata_runs_action where id_record = " + lRequest;
    out.println(delete + "<br>");
    DB db = new DB(delete);

    lia.web.servlets.web.Utils.logRequest("/deletion/del_request.jsp?id="+lRequest, 1, request);
%>

<script type="text/javascript">
    window.history.back();
</script>