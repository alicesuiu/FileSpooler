<%@ page import="lazyj.RequestWrapper" %>
<%@ page import="alien.catalogue.LFN" %>
<%@ page import="alien.catalogue.LFNUtils" %>
<%
    String clientAddr = request.getRemoteAddr();

    if (!clientAddr.startsWith("10.162.36.") && 		// EPN IB interfaces
                    !clientAddr.equals("128.141.19.252")		// alihlt-gw-prod.cern.ch
    ) {
        lia.web.servlets.web.Utils.logRequest("/epn2eos/lfncheck.jsp?DENIED=" + clientAddr, 0, request);
        response.sendError(HttpServletResponse.SC_FORBIDDEN, "Client not authorized");
        return;
    }

    response.setContentType("text/plain");

    final RequestWrapper rw = new RequestWrapper(request);

    final String curl = rw.gets("curl");

    // sanity check
    if (curl.length() == 0 || curl.isBlank()) {
        response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Wrong parameter");
        return;
    }

    LFN existing = LFNUtils.getLFN(curl);

    if (existing != null) {
        response.setStatus(HttpServletResponse.SC_FOUND);
    }
    else {
        response.setStatus(HttpServletResponse.SC_NOT_FOUND);
    }
%>
