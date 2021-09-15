package spooler;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
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

	private static final String URL = "http://alimonitor.cern.ch/epn2eos/daqreg.jsp";

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

		if (element.getMd5() == null)
			urlParam += encode("md5") + "=" + encode("missing");
		else
			urlParam += encode("md5") + "=" + encode(element.getMd5());

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

			logger.log(Level.FINE, "status code HTTP: " + status);

			connection.disconnect();
		}
		catch (IOException e) {
			logger.log(Level.WARNING, "Communication error", e);
			status = HttpServletResponse.SC_SERVICE_UNAVAILABLE;
		}

		return new Pair<>(Integer.valueOf(status), response);
	}

	private static void onSuccess(FileElement element, boolean isMetadata) {
        logger.log(Level.INFO, "Successfully registered: " + element.getCurl());
	    if (isMetadata) {
            Main.nrMetaFilesReg.getAndIncrement();
            logger.log(Level.INFO, "Total number of metadata files successfully registered: "
                    + Main.nrMetaFilesReg.get());
            monitor.incrementCacheHits("metadata_registered_files");

            if (!element.getFile().delete())
                logger.log(Level.WARNING, "Could not delete metadata file " + element.getMetaFilePath());
        } else {
            Main.nrDataFilesReg.getAndIncrement();
            logger.log(Level.INFO, "Total number of data files successfully registered: "
                    + Main.nrDataFilesReg.get());
            monitor.incrementCacheHits("data_registered_files");
        }
	}

	private static void onFail(FileElement element, String msg, int status, boolean isMetadata) {
	    if (isMetadata) {
            Main.nrMetaFilesRegFailed.getAndIncrement();
            logger.log(Level.INFO, "Total number of metadata files whose registration failed: "
                    + Main.nrMetaFilesRegFailed.get());
            monitor.incrementCacheMisses("metadata_registered_files");
        } else{
            Main.nrDataFilesRegFailed.getAndIncrement();
            logger.log(Level.INFO, "Total number of data files whose registration failed: "
                    + Main.nrDataFilesRegFailed.get());
            monitor.incrementCacheMisses("data_registered_files");
        }

        logger.log(Level.INFO, String.valueOf(msg));
        if (status == HttpServletResponse.SC_BAD_REQUEST
				|| status == HttpServletResponse.SC_FORBIDDEN
				|| status == HttpServletResponse.SC_CONFLICT) {
			String path = Main.spoolerProperties.gets("errorDir", Main.defaultErrorDir)
					+ element.getMetaFilePath().substring(element.getMetaFilePath().lastIndexOf('/'));
			Main.moveFile(logger, element.getMetaFilePath(), path);
		}
		else {
			element.computeDelay();
			Main.registrationWatcher.addElement(element);
		}
	}

	private static boolean register(FileElement element, boolean isMetadata) {
		Pair<Integer, String> response = sendRequest(element);
		int status = response.getFirst().intValue();
		String msg = response.getSecond();

		if (status == HttpServletResponse.SC_OK || status == HttpServletResponse.SC_CREATED) {
			onSuccess(element, isMetadata);
			return true;
		}

		onFail(element, msg, status, isMetadata);
		return false;
	}

	private void registerFile() {
		boolean status = register(toRegister, false);

		if (status) {
			FileElement metadataFile = new FileElement(
					null,
					toRegister.getSurl().concat(".meta"),
					new File(toRegister.getMetaFilePath()).length(),
					toRegister.getRun(),
					GUIDUtils.generateTimeUUID(),
					new File(toRegister.getMetaFilePath()).lastModified(),
					toRegister.getLHCPeriod(),
					null,
					0,
					toRegister.getMetaFilePath(),
					toRegister.getType(),
					toRegister.getCurl().concat(".meta"),
					toRegister.getSeName(),
					toRegister.getSeioDaemons(),
					null);
			register(metadataFile, true);
		}
	}

	@Override
	public void run() {
		logger.log(Level.INFO, "Total number of files registered in parallel: "
				+ Main.nrFilesOnRegister.incrementAndGet());

		try {
			registerFile();
		}
		finally {
			Main.nrFilesOnRegister.decrementAndGet();
		}
	}
}
