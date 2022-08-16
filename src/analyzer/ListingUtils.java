package analyzer;

import alien.config.ConfigUtils;
import alien.io.xrootd.XrootdFile;
import alien.io.xrootd.XrootdListing;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

class ListingUtils {
     static String server = Main.listingProperties.gets("seioDaemons", Main.defaultseioDaemons)
            .substring(Main.listingProperties.gets("seioDaemons",
                    Main.defaultseioDaemons).lastIndexOf('/') + 1);
     static String statFileName = "/data/epn2eos_tool/stat_file_size";
     static String statRootDirsFileName = "/data/epn2eos_tool/stat_root_dirs";
    private static Logger logger = ConfigUtils.getLogger(ListingUtils.class.getCanonicalName());

    /*static String getParentName(String path) {
        Path parent = Paths.get(path).getParent();
        return parent.getName(parent.getNameCount() - 1).toString();
    }

    static String getFileName(String dirName, String parent) {
        String localFile = Main.listingProperties.gets("localFile", Main.defaultLocalFile);
        String name = localFile.substring(0, localFile.lastIndexOf('.')) + "-";
        name += (parent  != null) ? (parent + "-" + dirName) : dirName;
        String extension = localFile.substring(localFile.lastIndexOf(".") + 1);
        return String.format("%s.%s", name, extension);
    }*/

    static Set<String> getScanDirs(String fileName) {
        Set<String> scanDirs = new HashSet<>();
        try(BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            String path;
            while ((path = reader.readLine()) != null) {
                scanDirs.add(path.trim());
            }
        } catch (IOException e) {
            logger.log(Level.WARNING, "Caught exception while trying to read from file " + fileName);
        }
        return scanDirs;
    }

    static Set<XrootdFile> getFirstLevelDirs(String path) throws IOException {
        XrootdListing listing = new XrootdListing(server, path, null);
        return listing.getDirs();
    }
}
