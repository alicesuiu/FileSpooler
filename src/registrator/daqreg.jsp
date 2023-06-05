<%@page import="lazyj.cache.ExpirationCache"%>
<%@ page import="lazyj.*,alien.catalogue.*,alien.user.*"%>
<%@ page import="java.util.logging.Logger"%>
<%@ page import="alien.config.ConfigUtils"%>
<%@ page import="lazyj.commands.CommandOutput"%>
<%@ page import="lia.Monitor.Store.Fast.DB" %>
<%@ page import="java.util.Date" %>
<%@ page import="java.io.PrintWriter" %>
<%@ page import="java.io.FileWriter" %>
<%@ page import="java.util.UUID" %>
<%@ page import="utils.ExpireTime" %>
<%@ page import="alien.io.TransferUtils" %>
<%@ page import="alien.se.SE" %>
<%@ page import="alien.se.SEUtils" %>

<%!private static final Object lock = new Object();
	private static PrintWriter pwLog = null;
	private static final Logger logger = ConfigUtils.getLogger("daqreg");
	private static final AliEnPrincipal OWNER = UserFactory.getByUsername("alidaq");

	private static final synchronized void logMessage(final String message){
		final Date d = new Date();

		if (pwLog==null){
			try{
				pwLog = new PrintWriter(new FileWriter("/home/monalisa/MLrepository/logs/daqreg.log", true));
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
			System.err.println("daqreg.jsp: "+d+" : "+message);
	}

	private static final ExpirationCache<String, String> mkDirsHistory = new ExpirationCache<>(1000);%>
<%
	int code = HttpServletResponse.SC_OK;
	String curl = null, pfn = null;
	long size = 0;
	try {
		String clientAddr = request.getRemoteAddr();

		if (
				!clientAddr.startsWith("10.162.36.") &&        // EPN IB interfaces
						!clientAddr.equals("128.141.19.252")        // alihlt-gw-prod.cern.ch
		) {
			lia.web.servlets.web.Utils.logRequest("/epn2eos/daqreg.jsp?DENIED=" + clientAddr, 0, request);
			logMessage("Client not authorized: " + clientAddr);
			response.sendError(HttpServletResponse.SC_FORBIDDEN, "Client not authorized");
			code = HttpServletResponse.SC_FORBIDDEN;
			return;
		}

		response.setContentType("text/plain");

		final RequestWrapper rw = new RequestWrapper(request);

		curl = rw.gets("curl");
		final String surl = rw.gets("surl");
		final String seName = rw.gets("seName");
		final String seioDaemons = rw.gets("seioDaemons");
		final String guid = rw.gets("guid").trim();
		final String period = rw.gets("LHCPeriod");
		size = rw.getl("size");
		long ctime = rw.getl("ctime");
		String md5 = rw.gets("md5");
		String TFOrbits = rw.gets("TFOrbits");
		String hostname = rw.gets("hostname");
		String persistent = rw.gets("persistent");
		final String type = rw.gets("type");

		// sanity check
		if (size <= 0 || surl.length() == 0
				|| md5.length() == 0 || period.length() == 0
				|| curl.length() == 0 || seName.length() == 0
				|| seioDaemons.length() == 0 || TFOrbits.length() == 0
				|| persistent.length() == 0 || type.length() == 0) {
			logMessage("Wrong parameters:\nsize: "+size+"\nsurl: "+surl+"\nmd5: "+md5+"\n" +
					"period: "+period+"\ncurl: "+curl+"\nseName: "+seName+"\n" +
					"seioDaemons: "+seioDaemons+"\nTFOrbits.length(): "+TFOrbits.length()+"\n" +
					"persistent: "+persistent+"\ntype: "+type);
			response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Wrong parameters");
			code = HttpServletResponse.SC_BAD_REQUEST;
			return;
		}

		if (ctime <= 0)
			ctime = GUIDUtils.epochTime(UUID.fromString(guid));

		if (md5.contains("missing"))
			md5 = null;

		if (TFOrbits.contains("missing"))
			TFOrbits = null;

		if (hostname.contains("missing"))
			hostname = null;

		if (persistent.contains("missing"))
			persistent = null;

		String client;
		if (hostname == null || hostname.isBlank())
			client = clientAddr;
		else
			client = hostname;

		pfn = seioDaemons + "/" + surl;
		final String msg = curl + ", " + pfn + ", " + guid + ", " + md5
				+ ", " + size + ", " + seName + ", " + ctime + ", " + TFOrbits;

		int idx = curl.indexOf(period);

		if (idx > 0) {
			final String sPartitionDir = curl.substring(0, idx + period.length());
			//final String sPartitionDir = curl.substring(0, idx);

			if (mkDirsHistory.get(sPartitionDir) == null) {
				synchronized (lock) {
					if (LFNUtils.mkdirs(OWNER, sPartitionDir) == null) {
						logMessage("Cannot create directory: " + sPartitionDir);
						response.sendError(HttpServletResponse.SC_FORBIDDEN, "Cannot create directory: " + sPartitionDir);
						code = HttpServletResponse.SC_FORBIDDEN;
						return;
					}

					final CommandOutput co = alien.pool.AliEnPool.executeCommand("admin", "moveDirectory " + sPartitionDir, true);
					alien.catalogue.CatalogueUtils.invalidateIndexTableCache();
					logMessage("daqreg.jsp :  moveDirectory (" + sPartitionDir + "):\n" + co);
				}
				mkDirsHistory.put(sPartitionDir, sPartitionDir, 1000 * 60 * 10);
			}
		}

		LFN existing = LFNUtils.getLFN(curl);

		if (existing != null) {
			logMessage(client + ": File was already registered: " + curl);

			if (existing.size != size) {
				response.sendError(HttpServletResponse.SC_CONFLICT, "File " + curl
						+ " already exists in the catalogue with a different size (" + existing.size + " vs " + size + ")");
				code = HttpServletResponse.SC_CONFLICT;
				return;
			}
		} else {
			boolean done = false;
			try {
				synchronized (lock) {
					done = Register.register(curl, pfn, guid, md5, size, seName, OWNER);
				}
			} catch (Throwable t) {
				logMessage(client + ": Caught exception: " + t.getMessage());
				response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, "Caught exception : "
						+ t + " (" + t.getMessage() + ")");
				code = HttpServletResponse.SC_SERVICE_UNAVAILABLE;
				return;
			}

			if (!done) {
				logMessage(client + ": Registering failed for: " + msg);
				response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, "Registering by JAPI failed for:\n" + msg);
				code = HttpServletResponse.SC_SERVICE_UNAVAILABLE;
				return;
			}

			logMessage(client + ": Successfuly registered for: " + msg);
		}

		if (persistent != null) {
			LFN lfn = LFNUtils.getLFN(curl);
			ExpireTime expTime = new ExpireTime();
			expTime.setDays(Integer.parseInt(persistent));
			LFNUtils.setExpireTime(lfn, expTime, false);
		}

		final DB db = new DB();
		String insert, update;

		if (!db.query("SELECT 123 FROM rawdata WHERE lfn='" + Format.escSQL(curl) + "';")) {
			logMessage("Repository: cannot query database");
			response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, "Repository: cannot query database");
			code = HttpServletResponse.SC_SERVICE_UNAVAILABLE;
			return;
		}

		if (TFOrbits != null) {
			insert = "INSERT INTO rawdata (lfn, addtime, size, pfn, file_type, TFOrbits) VALUES ('"
					+ Format.escSQL(curl) + "', " + ctime + ", " + size + ", '" + Format.escSQL(surl)
					+ "', '" + Format.escSQL(type) + "', ARRAY[" + Format.escSQL(TFOrbits) + "]" + ");";
			update = "UPDATE rawdata SET size=" + size + ", pfn='" + Format.escSQL(surl)
					+ "', addtime=" + ctime + ", file_type='" + Format.escSQL(type) + "', TFOrbits=ARRAY[" + Format.escSQL(TFOrbits)
					+ "] WHERE lfn='" + Format.escSQL(curl) + "' AND (size IS NULL OR size!=" + size
					+ " OR pfn IS NULL OR pfn!='" + Format.escSQL(surl) + "' OR addtime IS NULL OR addtime!=" + ctime
					+ " OR file_type IS NULL OR file_type!='" + Format.escSQL(type) + "');";
		} else {
			insert = "INSERT INTO rawdata (lfn, addtime, size, pfn, file_type) VALUES ('"
					+ Format.escSQL(curl) + "', " + ctime + ", " + size + ", '" + Format.escSQL(surl)
					+ "', '" + Format.escSQL(type) + "');";
			update = "UPDATE rawdata SET size=" + size + ", pfn='" + Format.escSQL(surl)
					+ "', addtime=" + ctime + ", file_type='" + Format.escSQL(type) + "' WHERE lfn='" + Format.escSQL(curl)
					+ "' AND (size IS NULL OR size!=" + size + " OR pfn IS NULL OR pfn!='" + Format.escSQL(surl) +
					"' OR addtime IS NULL OR addtime!=" + ctime + " OR file_type IS NULL OR file_type!='" + Format.escSQL(type) + "');";
		}

		if (db.geti(1) == 123) {
			if (db.syncUpdateQuery(update)) {
				if (db.getUpdateCount() == 0)
					logMessage(client + ": Repository: file existed with all details: " + curl);
				else
					logMessage(client + ": Repository: file existed but was updated: " + curl);
			} else {
				logMessage(client + ": Repository: cannot update the existing file: " + curl);
				response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, "Repository: cannot update the existing file: " + curl);
				code = HttpServletResponse.SC_SERVICE_UNAVAILABLE;
				return;
			}
		} else {
			if (db.syncUpdateQuery(insert)) {
				if (db.getUpdateCount() == 0) {
					logMessage(client + ": Repository: cannot insert new file: " + curl);
					response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, "Repository: cannot insert new file: " + curl);
					code = HttpServletResponse.SC_SERVICE_UNAVAILABLE;
					return;
				}
				logMessage(client + ": Repository: new file successfully inserted: " + curl);
			} else {
				logMessage(client + ": Repository: cannot insert new file: " + curl);
				response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, "Repository: cannot insert new file: " + curl);
				code = HttpServletResponse.SC_SERVICE_UNAVAILABLE;
				return;
			}
		}

		String defaultSEName = "ALICE::CERN::EOSALICEO2";
		String fallbackSEName = "ALICE::CERN::EOSP2";
		if (seName.contains(fallbackSEName)) {
			SE se = SEUtils.getSE(defaultSEName);
			LFN lfn = LFNUtils.getLFN(curl);
			TransferUtils.mirror(lfn, se, fallbackSEName, 100);
		}
	} finally {
		lia.web.servlets.web.Utils.logRequest("/epn2eos/daqreg.jsp?lfn=" + curl + "&pfn=" + pfn + "&size=" + size, code, request);
	}
%>
