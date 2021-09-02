package spooler;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author asuiu
 * @since August 24, 2021
 */
public class Registrator implements Runnable {
    private final Logger logger = ConfigUtils.getLogger(Registrator.class.getCanonicalName());
    private final Monitor monitor = MonitorFactory.getMonitor(Registrator.class.getCanonicalName());

    private BlockingQueue<FileElement> filesToRegister;
    private final String URL = "http://172.30.0.1:8080/daqreg.jsp";

    Registrator(BlockingQueue<FileElement> filesToRegister) {
        this.filesToRegister = filesToRegister;
    }

    private String encode(final String s){
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }

    private String getURLParam(FileElement element) {
        String urlParam = "";

        urlParam += encode("curl") + "=" + encode(element.getCurl()) + "&";
        urlParam += encode("surl") + "=" + encode(element.getSurl()) + "&";
        urlParam += encode("seName") + "=" + encode(element.getSeName()) + "&";
        urlParam += encode("guid") + "=" + encode(element.getGuid().toString()) + "&";
        urlParam += encode("size") + "=" + encode(String.valueOf(element.getSize())) + "&";

        if (element.getMd5() == null)
            urlParam += encode("md5") + "=" + encode("missing");
        else
            urlParam += encode("md5") + "=" + encode(element.getMd5());

        return urlParam;
    }

    private void registerFile(FileElement element) {
        int status;
        String inputLine;
        StringBuffer content = new StringBuffer();
        String urlParam = getURLParam(element);

        if (urlParam.length() > 0) {
            try {
                URL url = new URL(URL);
                HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                connection.setRequestMethod("GET");
                connection.setDoOutput(true);
                connection.setConnectTimeout(1000 * 60);

                try (OutputStreamWriter writer = new OutputStreamWriter(connection.getOutputStream())) {
                    writer.write(urlParam);
                    writer.flush();
                }

                status = connection.getResponseCode();

                if (status == HttpServletResponse.SC_OK || status == HttpServletResponse.SC_CREATED) {
                    Main.nrFilesRegistered.getAndIncrement();
                    logger.log(Level.INFO, "Successfuly registered: " + element.getCurl());
                    logger.log(Level.INFO, "Total number of files successfully registered: "
                            + Main.nrFilesRegistered.get());
                    monitor.incrementCounter("files_successfully_registered");

                    /*if (!new File(element.getMetaFilePath()).delete()) {
                    logger.log(Level.WARNING, "Could not delete metadata file " + element.getMetaFilePath());
                    */
                } else {
                    try (BufferedReader reader = new BufferedReader(
                            new InputStreamReader(connection.getErrorStream()))) {

                        content.setLength(0);
                        while ((inputLine = reader.readLine()) != null) {
                            content.append(inputLine);
                        }
                    }

                    Main.nrFilesRegFailed.getAndIncrement();
                    logger.log(Level.INFO, String.valueOf(content));
                    logger.log(Level.INFO, "Total number of files whose registration failed: "
                            + Main.nrFilesRegFailed.get());
                    monitor.incrementCounter("files_registration_failed");
                }

                connection.disconnect();
            } catch (IOException e) {
                logger.log(Level.WARNING, "Communication error", e);
            }
        }
    }

    @Override
    public void run() {
        FileElement file;

        try {
            while (true) {
                logger.log(Level.INFO, "Total number of files to be registered: " + filesToRegister.size());

                file = filesToRegister.take();
                if (file == null)
                    continue;

                monitor.incrementCounter("files_registered");

                registerFile(file);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
