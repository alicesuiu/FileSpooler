package metacreator;

import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.PFN;
import alien.config.ConfigUtils;
import alien.io.IOUtils;
import alien.io.protocols.Xrootd;
import alien.io.xrootd.XrootdFile;
import alien.se.SE;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MetaCreator implements Runnable {
    private BlockingQueue<XrootdFile> files;
    private static Logger logger = ConfigUtils.getLogger(MetaCreator.class.getCanonicalName());

    MetaCreator(BlockingQueue<XrootdFile> files) {
        this.files = files;
    }

    @Override
    public void run() {
        while(true) {
            try {
                XrootdFile file = files.take();
                logger.log(Level.INFO, "Metacreator thread processes the file: " + file.path);
                createMetadataFile(file);
            } catch (InterruptedException e) {
                logger.log(Level.WARNING, "MetaCreator thread was interrupted while waiting", e);
            }
        }
    }

    private void createMetadataFile(XrootdFile file) {
        String seName, seioDaemons, surl, curl, metaFileName, extension, tmpFile, md5Output, srcPath, destPath,
                run, period, lurl;
        UUID guid;
        long size, ctime;

        metaFileName = "meta-" + file.path.replace("/eos/aliceo2/ls2data/", "")
                .replace('/', '-') + ".done";

        srcPath = Main.metacreatorProperties.gets("metaDir", Main.defaultMetadataDir) + "/" + metaFileName;
        destPath = Main.metacreatorProperties.gets("registrationDir", Main.defaultRegistrationDir) + "/"
                + metaFileName;

        run = "499999";
        period = "LHC21z";
        lurl = destPath;

        seName = Main.metacreatorProperties.gets("seName", Main.defaultSEName);
        seioDaemons = Main.metacreatorProperties.gets("seioDaemons", Main.defaultseioDaemons);
        surl = file.path;
        curl = "/alice/data/2021/" + period + "/" + file.path.replace("/eos/aliceo2/ls2data/", "");
        guid = GUIDUtils.generateTimeUUID();
        size = file.size;
        ctime = file.date.getTime();

        tmpFile = "/data/epn2eos_tool/tmp_file_" + guid;

        try(FileWriter writeFile = new FileWriter(srcPath, true)) {
            writeFile.write("seName" + ": " + seName + "\n");
            writeFile.write("seioDaemons" + ": " + seioDaemons + "\n");
            writeFile.write("surl" + ": " + surl + "\n");
            writeFile.write("curl" + ": " + curl + "\n");
            writeFile.write("guid" + ": " + guid + "\n");
            writeFile.write("size" + ": " + size + "\n");
            writeFile.write("ctime" + ": " + ctime + "\n");
            writeFile.write("run" + ": " + run + "\n");
            writeFile.write("LHCPeriod" + ": " + period + "\n");
            writeFile.write("lurl" + ": " + lurl + "\n");

            SE se = new SE(seName, 1, "", "",seioDaemons);
            GUID guidG = new GUID(guid);
            guidG.size = size;
            PFN pfn = new PFN(seioDaemons + "/" + surl, guidG, se);

            (new Xrootd()).get(pfn, new File(tmpFile));
            md5Output = IOUtils.getMD5(new File(tmpFile));
            logger.log(Level.INFO, "MD5 checksum for the file " + surl + " is " + md5Output);
            writeFile.write("md5" + ": " + md5Output + "\n");

            if (!new File(tmpFile).delete())
                logger.log(Level.WARNING, "Could not delete file " + tmpFile);

            Main.nrFilesCreated.getAndIncrement();
            logger.log(Level.INFO, "Total number of metadata files successfully created: "
                    + Main.nrFilesCreated.get());
        } catch (IOException e) {
            logger.log(Level.WARNING,"Caught exception while writing the metadata file for " + metaFileName, e);
        }

        //Main.moveFile(logger, srcPath, destPath);
    }
}
