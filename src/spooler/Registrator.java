package spooler;

import alien.catalogue.GUIDUtils;
import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.site.supercomputing.titan.Pair;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
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

    private Pair<Integer, String> sendRequest(FileElement element) {
        int status = -1;
        String response = "";

        try {
            URL url = new URL(URL);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setDoOutput(true);
            connection.setConnectTimeout(1000 * 60);

            String urlParam = getURLParam(element);

            try (OutputStreamWriter writer = new OutputStreamWriter(connection.getOutputStream())) {
                writer.write(urlParam);
                writer.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }

            status = connection.getResponseCode();
            response  = connection.getResponseMessage();

            connection.disconnect();
        } catch (IOException e) {
            logger.log(Level.WARNING, "Communication error", e);
            filesToRegister.add(element);
        }

        return new Pair(status, response);
    }

    private void registerFile(FileElement element) throws IOException {
        int status;
        String msg;

        Pair<Integer, String> response = sendRequest(element);
        status = response.getFirst();
        msg = response.getSecond();

        if (status == HttpServletResponse.SC_OK || status == HttpServletResponse.SC_CREATED) {
            Main.nrFilesRegistered.getAndIncrement();
            logger.log(Level.INFO, "Successfuly registered: " + element.getCurl());
            logger.log(Level.INFO, "Total number of files successfully registered: "
                    + Main.nrFilesRegistered.get());
            monitor.incrementCacheHits("registered_files");

            FileElement metadataFile = new FileElement(
                    null,
                    element.getSurl().concat(".meta"),
                    new File(element.getMetaFilePath()).length(),
                    element.getRun(),
                    GUIDUtils.generateTimeUUID(),
                    new File(element.getMetaFilePath()).lastModified(),
                    element.getLHCPeriod(),
                    null,
                    0,
                    element.getMetaFilePath(),
                    element.getType(),
                    element.getCurl().concat(".meta"),
                    element.getSeName(),
                    element.getSeioDaemons(),
                    null
            );

            sendRequest(metadataFile);
            logger.log(Level.INFO, "Successfuly registered: " + metadataFile.getCurl());

            if (!new File(element.getMetaFilePath()).delete())
                logger.log(Level.WARNING, "Could not delete metadata file " + element.getMetaFilePath());
        } else {
            Main.nrFilesRegFailed.getAndIncrement();
            logger.log(Level.INFO, String.valueOf(msg));
            logger.log(Level.INFO, "Total number of files whose registration failed: "
                    + Main.nrFilesRegFailed.get());
            monitor.incrementCacheMisses("registered_files");
            String path = Main.spoolerProperties.gets("errorDir", Main.defaultErrorDir)
                    + element.getMetaFilePath().substring(element.getMetaFilePath().lastIndexOf('/'));
            Files.move(Paths.get(element.getMetaFilePath()), Paths.get(path), StandardCopyOption.REPLACE_EXISTING);
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

                registerFile(file);
            }
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }
}
