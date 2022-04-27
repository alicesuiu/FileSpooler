package analyzer;

import alien.io.xrootd.XrootdFile;
import alien.io.xrootd.XrootdListing;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;

class ListingUtils {
    static String getParentName(String path) {
        Path parent = Paths.get(path).getParent();
        return parent.getName(parent.getNameCount() - 1).toString();
    }

    static String getFileName(String dirName, String parent) {
        String localFile = ListingMain.listingProperties.gets("localFile", ListingMain.defaultLocalFile);
        String name = localFile.substring(0, localFile.lastIndexOf('.')) + "-";
        name += (parent  != null) ? (parent + "-" + dirName) : dirName;
        String extension = localFile.substring(localFile.lastIndexOf(".") + 1);
        return String.format("%s.%s", name, extension);
    }

    static Set<XrootdFile> getFirstDirs(String path, String fileName) throws IOException {
        XrootdListing listing = new XrootdListing(ListingMain.server, path, null);
        Set<XrootdFile> files = listing.getFiles();

        if (!files.isEmpty()) {
            try (FileWriter writer = new FileWriter(fileName, true)) {
                for (XrootdFile f : files) {
                    writer.write(f.path + ", " + f.size + "\n");
                }
            }
        }

        return listing.getDirs();
    }
}
