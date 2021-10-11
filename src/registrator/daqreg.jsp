<%@page import="lazyj.cache.ExpirationCache"%>
<%@ page import="lazyj.*,alien.catalogue.*,alien.user.*"%>
<%@ page import="java.util.logging.Logger"%>
<%@ page import="alien.config.ConfigUtils"%>
<%@ page import="java.util.logging.Level"%>
<%@ page import="lazyj.commands.CommandOutput"%>
<%@ page import="lia.Monitor.Store.Fast.DB" %>

<%!private static final Object lock = new Object();
	private static final Logger logger = ConfigUtils.getLogger("daqreg");
	private static final AliEnPrincipal OWNER = UserFactory.getByUsername("alidaq");

	private static final ExpirationCache<String, String> mkDirsHistory = new ExpirationCache<>(1000);%>
<%
String clientAddr = request.getRemoteAddr();

if (
    !clientAddr.startsWith("10.162.36.") && 		// EPN IB interfaces
    !clientAddr.equals("128.141.19.252")		// alihlt-gw-prod.cern.ch
) {
	lia.web.servlets.web.Utils.logRequest("/epn2eos/daqreg.jsp?DENIED=" + clientAddr, 0, request);
	logger.log(Level.WARNING, "Client not authorized");
	response.sendError(HttpServletResponse.SC_FORBIDDEN, "Client not authorized");
	return;
}

response.setContentType("text/plain");

final RequestWrapper rw = new RequestWrapper(request);

final String curl = rw.gets("curl");
final String surl = rw.gets("surl");
final String seName = rw.gets("seName");
final String seioDaemons = rw.gets("seioDaemons");
final String guid = rw.gets("guid").trim();
final String period = rw.gets("LHCPeriod");
final long size = rw.getl("size");
final long ctime = rw.getl("ctime");
String md5 = rw.gets("md5");
String TFOrbits = rw.gets("TFOrbits");

// sanity check
if (size <= 0 || surl.length() == 0
		|| md5.length() == 0 || period.length() == 0
		|| curl.length() == 0 || seName.length() == 0
		|| seioDaemons.length() == 0 || ctime <= 0
		|| TFOrbits.length() == 0) {
	logger.log(Level.WARNING, "Wrong parameters");
	response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Wrong parameters");
	return;
}

if (md5.contains("missing"))
	md5 = null;

if (TFOrbits.contains("missing"))
	TFOrbits = null;

final String pfn = seioDaemons + "/" + surl;
final String msg = "File : " + curl
		+ "\nPFN: " + pfn
		+ "\nGUID: " + guid
		+ "\nMD5: " + md5
		+ "\nSize: " + size
		+ "\nSE: " + seName
		+ "\nCtime: " + ctime
		+ "\nTFOrbits: " + TFOrbits;

logger.log(Level.INFO, msg);

int idx = curl.indexOf(period);

if (idx > 0) {
	final String sPartitionDir = curl.substring(0, idx);

	if (mkDirsHistory.get(sPartitionDir) == null) {
		synchronized (lock) {
			if (LFNUtils.mkdirs(OWNER, sPartitionDir) == null) {
				logger.log(Level.WARNING, "Cannot create directory: " + sPartitionDir);
				response.sendError(HttpServletResponse.SC_FORBIDDEN, "Cannot create directory: " + sPartitionDir);
				return;
			}

			final CommandOutput co = alien.pool.AliEnPool.executeCommand("admin", "moveDirectory " + sPartitionDir, true);
			alien.catalogue.CatalogueUtils.invalidateIndexTableCache();
			logger.log(Level.INFO, "daqreg.jsp :  moveDirectory (" + sPartitionDir + "):\n" + co);
		}
		mkDirsHistory.put(sPartitionDir, sPartitionDir, 1000 * 60 * 10);
	}
}

LFN existing = LFNUtils.getLFN(curl);

if (existing != null) {
	logger.log(Level.INFO, "File was already registered");

	if (existing.size != size)
		response.sendError(HttpServletResponse.SC_CONFLICT, "File " + curl
		+ " already exists in the catalogue with a different size (" + existing.size + " vs " + size + ")");
	else
		response.setStatus(HttpServletResponse.SC_OK);
}
else {
	boolean done = false;
	try {
		synchronized (lock) {
			done = Register.register(curl, pfn, guid, md5, size, seName, OWNER);
		}
	}
	catch (Throwable t) {
		logger.log(Level.WARNING, "Caught exception", t);
		response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, "Caught exception : "
		+ t + " (" + t.getMessage() + ")");
	}

	if (!done) {
		logger.log(Level.INFO, "Registering failed for:\n" + msg);
		response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, "Registering by JAPI failed for:\n" + msg);
	}
	else {
		logger.log(Level.INFO, "Successfuly registered for:\n" + msg);
		response.setStatus(HttpServletResponse.SC_CREATED);
	}
}

int code = response.getStatus();

if (TFOrbits != null) {

	final DB db = new DB();

	if (!db.query("SELECT 123 FROM rawdata WHERE lfn='" + Format.escSQL(curl) + "';")) {
		logger.log(Level.WARNING, "Repository: cannot query database");
		return;
	}

	if (db.geti(1) == 123) {
		final String q = "UPDATE rawdata SET size=" + size + ", pfn='" + Format.escSQL(surl)
				+ "', addtime=" + ctime + ", TFOrbits=ARRAY[" + Format.escSQL(TFOrbits)
				+ "] WHERE lfn='" + Format.escSQL(curl) + "' AND (size IS NULL OR size!=" + size
				+ " OR pfn IS NULL OR pfn!='" + Format.escSQL(surl) + "' OR addtime IS NULL OR addtime!=" + ctime + ");";

		if (db.syncUpdateQuery(q)) {
			if (db.getUpdateCount() == 0) {
				logger.log(Level.WARNING, "Repository: file existed with all details");
				code = 1;
			} else {
				logger.log(Level.WARNING, "Repository: file existed but was updated");
				code = 2;
			}
		} else {
			logger.log(Level.WARNING, "Repository: cannot update the existing file");
			code = 3;
		}
	} else {
		if (db.syncUpdateQuery("INSERT INTO rawdata (lfn, addtime, size, pfn, TFOrbits) VALUES ('"
				+ Format.escSQL(curl) + "', " + ctime + ", " + size + ", '" + Format.escSQL(surl)
				+ ", ARRAY[" + Format.escSQL(TFOrbits) + "]" + "');")) {
			if (db.getUpdateCount() == 0) {
				logger.log(Level.WARNING, "Repository: cannot insert new file");
				code = 4;
			} else {
				logger.log(Level.WARNING, "Repository: new file successfully inserted");
				code = 5;
			}
		} else {
			logger.log(Level.WARNING, "Repository: cannot insert new file");
			code = 6;
		}
	}
}

lia.web.servlets.web.Utils.logRequest("/epn2eos/daqreg.jsp?lfn=" + curl + "&pfn=" + pfn + "&size=" + size, code, request);
%>
