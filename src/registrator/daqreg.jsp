<%@ page import="lazyj.*,alien.catalogue.*,alien.user.*,java.io.*"%>
<%@ page import="alien.se.SEUtils" %>
<%@ page import="java.util.logging.Logger" %>
<%@ page import="alien.config.ConfigUtils" %>
<%@ page import="java.util.logging.Level" %>
<%!
    private static final Object lock = new Object();
    private final Logger logger = ConfigUtils.getLogger("daqreg");
    private static final AliEnPrincipal OWNER = UserFactory.getByUsername("jalien");

%><%
    /*String clientAddr = request.getRemoteAddr();

    if (!clientAddr.equals("137.138.116.104") && !clientAddr.equals("2001:1458:201:b4b9::100:50") &&	// aldaqgw01-gpn03.cern.ch
            !clientAddr.equals("137.138.116.101") // aldaqgw02-gpn03.cern.ch
            && !clientAddr.startsWith("10.161.34.")
    ) {
        lia.web.servlets.web.Utils.logRequest("/work/daqreg.jsp?DENIED=" + clientAddr, 0, request);
        out.println("err:access denied to "+clientAddr);
        return;
    }*/

    response.setContentType("text/plain");

    final RequestWrapper rw = new RequestWrapper(request);

    final String curl = rw.gets("curl");
    final String surl = rw.gets("surl");
    final String seName = rw.gets("seName");
    final String guid = rw.gets("guid").trim();
    String md5 = rw.gets("md5");
    final long lSize = rw.getl("size");

    // sanity check
    if (lSize <= 0 || surl.length() == 0
            || md5.length() <= 0
            || curl.length() == 0 || seName.length() == 0) {
        logger.log(Level.WARNING, "Wrong parameters");
        response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Wrong parameters");
        return;
    }

    if (md5.contains("missing"))
        md5 = "";

    logger.log(Level.INFO, "File properties : " + curl
            + "\nPFN: " + SEUtils.getSE(seName).seioDaemons + "/" + surl
            + "\nGUID: " + guid
            + "\nMD5: " + md5
            + "\nSize: " + lSize
            + "\nSE: " + seName);

    final File cFile = new File(curl);
    if (cFile.exists()) {
        logger.log(Level.INFO, "File was already registered");
        response.setStatus(HttpServletResponse.SC_OK);
    } else {
        boolean done = false;
        try {
            synchronized(lock) {
                done = Register.register(curl, SEUtils.getSE(seName).seioDaemons + "/" + surl, guid, md5, lSize, seName, OWNER);
            }
        } catch (Throwable t) {
            logger.log(Level.WARNING, "Caught exception : " + t + " (" + t.getMessage() + ")");
            response.sendError(HttpServletResponse.SC_PRECONDITION_FAILED, "Caught exception : " + t + " (" + t.getMessage() + ")");
            t.printStackTrace();
        }

        if (!done) {
            logger.log(Level.INFO, "Registering by JAPI failed for:\nFile : " + curl
                    + "\nPFN: " + SEUtils.getSE(seName).seioDaemons + "/" + surl
                    + "\nGUID: " + guid
                    + "\nMD5: " + md5
                    + "\nSize: " + lSize
                    + "\nSE: " + seName);

            response.sendError(HttpServletResponse.SC_EXPECTATION_FAILED, "Registering by JAPI failed for:\nFile : " + curl
                    + "\nPFN: " + SEUtils.getSE(seName).seioDaemons + "/" + surl
                    + "\nGUID: " + guid
                    + "\nMD5: " + md5
                    + "\nSize: " + lSize
                    + "\nSE: " + seName);
        } else {
            logger.log(Level.INFO, "Successfuly registered by JAPI : " + curl
                    + ", " + SEUtils.getSE(seName).seioDaemons + "/" + surl
                    + ", " + guid
                    + ", " + md5
                    + ", " + lSize
                    + ", " + seName);
            response.setStatus(HttpServletResponse.SC_CREATED);
        }
    }
%>
