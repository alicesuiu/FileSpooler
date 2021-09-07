<%@ page import="lazyj.*,alien.catalogue.*,alien.user.*"%>
<%@ page import="java.util.logging.Logger" %>
<%@ page import="alien.config.ConfigUtils" %>
<%@ page import="java.util.logging.Level" %>
<%@ page import="lazyj.commands.CommandOutput" %>
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
    final String seioDaemons = rw.gets("seioDaemons");
    final String guid = rw.gets("guid").trim();
    final String period = rw.gets("LHCPeriod");
    final long lSize = rw.getl("size");
    String md5 = rw.gets("md5");

    // sanity check
    if (lSize <= 0 || surl.length() == 0
            || md5.length() == 0 || period.length() == 0
            || curl.length() == 0 || seName.length() == 0
            || seioDaemons.length() == 0) {
        logger.log(Level.WARNING, "Wrong parameters");
        response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Wrong parameters");
        return;
    }

    if (md5.contains("missing"))
        md5 = null;

    final String pfn = seioDaemons + "/" + surl;
    final String msg = "File : " + curl
            + "\nPFN: " + pfn
            + "\nGUID: " + guid
            + "\nMD5: " + md5
            + "\nSize: " + lSize
            + "\nSE: " + seName;

    logger.log(Level.INFO, msg);

    final String sPartitionDir = curl.split(period)[0];

    synchronized (lock) {
        if (LFNUtils.mkdirs(OWNER, sPartitionDir) == null){
            logger.log(Level.WARNING, "Cannot create directory: " + sPartitionDir);
            response.sendError(HttpServletResponse.SC_FORBIDDEN, "Cannot create directory: " + sPartitionDir);
            return;
        }

       /* final CommandOutput co = AliEnPool.executeCommand("admin", "moveDirectory " + sPartitionDir, true);
        alien.catalogue.CatalogueUtils.invalidateIndexTableCache();
        logger.log(Level.INFO, "daqreg.jsp :  moveDirectory (" + sPartitionDir + "):\n" + co);*/
    }

    if (LFNUtils.getLFN(curl) != null) {
        logger.log(Level.INFO, "File was already registered");
        response.setStatus(HttpServletResponse.SC_OK);
    } else {
        boolean done = false;
        try {
            synchronized(lock) {
                done = Register.register(curl, pfn, guid, md5, lSize, seName, OWNER);
            }
        } catch (Throwable t) {
            logger.log(Level.WARNING, "Caught exception : " + t + " (" + t.getMessage() + ")");
            response.sendError(HttpServletResponse.SC_PRECONDITION_FAILED, "Caught exception : " + t + " (" + t.getMessage() + ")");
            t.printStackTrace();
        }

        if (!done) {
            logger.log(Level.INFO, "Registering failed for:\n" + msg);
            response.sendError(HttpServletResponse.SC_EXPECTATION_FAILED, "Registering by JAPI failed for:\n" + msg);
        } else {
            logger.log(Level.INFO, "Successfuly registered for:\n" + msg);
            response.setStatus(HttpServletResponse.SC_CREATED);
        }
    }
%>
