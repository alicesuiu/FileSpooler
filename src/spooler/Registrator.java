package spooler;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletResponse;

import alien.catalogue.GUIDUtils;
import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.site.supercomputing.titan.Pair;

/**
 * @author asuiu
 * @since August 24, 2021
 */
class Registrator implements Runnable {
	private static final Logger logger = ConfigUtils.getLogger(Registrator.class.getCanonicalName());
	private static final Monitor monitor = MonitorFactory.getMonitor(Registrator.class.getCanonicalName());
	private final FileElement toRegister;

	private static final String URL = "http://alimonitor.cern.ch/epn2eos/daqreg2.jsp";

	Registrator(final FileElement toRegister) {
		this.toRegister = toRegister;
	}

	private static String encode(final String s) {
		return URLEncoder.encode(s, StandardCharsets.UTF_8);
	}

	private static String getURLParam(FileElement element) {
		String urlParam = "";

		urlParam += encode("curl") + "=" + encode(element.getCurl()) + "&";
		urlParam += encode("surl") + "=" + encode(element.getSurl()) + "&";
		urlParam += encode("seName") + "=" + encode(element.getSeName()) + "&";
		urlParam += encode("seioDaemons") + "=" + encode(element.getSeioDaemons()) + "&";
		urlParam += encode("LHCPeriod") + "=" + encode(element.getLHCPeriod()) + "&";
		urlParam += encode("guid") + "=" + encode(element.getGuid().toString()) + "&";
		urlParam += encode("size") + "=" + encode(String.valueOf(element.getSize())) + "&";
		urlParam += encode("ctime") + "=" + encode(String.valueOf(element.getCtime() / 1000)) + "&";

		if (element.getMd5() == null)
			urlParam += encode("md5") + "=" + encode("missing") + "&";
		else
			urlParam += encode("md5") + "=" + encode(element.getMd5()) + "&";

		if (element.getTFOrbits() == null)
			urlParam += encode("TFOrbits") + "=" + encode("missing") + "&";
		else
			urlParam += encode("TFOrbits") + "=" + encode(element.getTFOrbits()) + "&";

		String hostname = ConfigUtils.getLocalHostname();
		if (hostname == null)
			hostname = "missing";
		urlParam += encode("hostname") + "=" + encode(hostname);

		return urlParam;
	}

	private static Pair<Integer, String> sendRequest(FileElement element) {
		int status = -1;
		String response = "";

		try {
			URL url = new URL(URL);
			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			connection.setRequestMethod("GET");
			connection.setDoOutput(true);
			connection.setConnectTimeout(1000 * 10);
			connection.setReadTimeout(1000 * 60 * 2);

			String urlParam = getURLParam(element);

			try (OutputStreamWriter writer = new OutputStreamWriter(connection.getOutputStream())) {
				writer.write(urlParam);
			}

			status = connection.getResponseCode();
			response = connection.getResponseMessage();

			connection.disconnect();
		}
		catch (IOException e) {
			logger.log(Level.WARNING, "Communication error", e.getMessage());
			status = HttpServletResponse.SC_SERVICE_UNAVAILABLE;
		}

		return new Pair<>(Integer.valueOf(status), response);
	}

	private static void onSuccess(FileElement element) {
        logger.log(Level.INFO, "Successfully registered: " + element.getCurl());

		Main.nrDataFilesReg.getAndIncrement();
		logger.log(Level.INFO, "Total number of data files successfully registered: "
				+ Main.nrDataFilesReg.get());
		monitor.incrementCacheHits("data_registered_files");

		if (!new File(element.getMetaFilePath()).delete())
			logger.log(Level.WARNING, "Could not delete metadata file " + element.getMetaFilePath());
	}

	private static void onFail(FileElement element, String msg, int status) {
		Main.nrDataFilesRegFailed.getAndIncrement();
		logger.log(Level.INFO, "Total number of data files whose registration failed: "
				+ Main.nrDataFilesRegFailed.get());
		monitor.incrementCacheMisses("data_registered_files");

        logger.log(Level.INFO, "Failed registration for: " + element.getFile().getAbsolutePath()
				+ ".\nMessage: " + msg + "\nStatus Code: " + status);

		FileElement metadataFile = new FileElement(
				null,
				element.getMetaSurl(),
				new File(element.getMetaFilePath()).length(),
				element.getRun(),
				GUIDUtils.generateTimeUUID(),
				new File(element.getMetaFilePath()).lastModified(),
				element.getLHCPeriod(),
				null,
				0,
				element.getMetaFilePath(),
				element.getType(),
				element.getMetaCurl(),
				element.getSeName(),
				element.getSeioDaemons(),
				null,
				true,
				null);

		if (!metadataFile.existFile())
			return;

        if (status == HttpServletResponse.SC_BAD_REQUEST
				|| status == HttpServletResponse.SC_FORBIDDEN
				|| status == HttpServletResponse.SC_CONFLICT) {
			String path = Main.spoolerProperties.gets("errorRegDir", Main.defaultErrorRegDir)
					+ element.getMetaFilePath().substring(element.getMetaFilePath().lastIndexOf('/'));
			Main.moveFile(logger, element.getMetaFilePath(), path);
			monitor.incrementCounter("error_files");
		}
		else {
			element.computeDelay();
			Main.registrationWatcher.addElement(element);
		}
	}

	private static boolean register(FileElement element) {
		Pair<Integer, String> response = sendRequest(element);
		int status = response.getFirst().intValue();
		String msg = response.getSecond();

		if (status == HttpServletResponse.SC_OK || status == HttpServletResponse.SC_CREATED) {
			onSuccess(element);
			return true;
		}

		onFail(element, msg, status);
		return false;
	}

	@Override
	public void run() {
		logger.log(Level.INFO, "Total number of files registered in parallel: "
				+ Main.nrFilesOnRegister.incrementAndGet());

		try {
			register(toRegister);
		}
		finally {
			Main.nrFilesOnRegister.decrementAndGet();
		}
	}
}
