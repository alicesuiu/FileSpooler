<%@ page import="lazyj.*,auth.*,java.security.cert.*,java.util.*" %><%

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
%>
<form action=add.jsp method=post>
    <table border=0 cellspacing=10 cellpadding=0>

        <tr>
            <td>Runs:</td>
            <td><input type=text name=runs value="" class=input_text></td>
        </tr>

        <tr>
            <td>&nbsp;</td>
            <td><input type=submit value="Next" class=input_text>
        </tr>
    </table>
</form>
<%
%>