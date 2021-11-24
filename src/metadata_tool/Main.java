package metadata_tool;

import alien.config.ConfigUtils;
import alien.io.xrootd.XrootdFile;
import alien.io.xrootd.XrootdListing;
import lazyj.ExtProperties;
import lazyj.Format;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class Main {
    private static ExtProperties creatorProperties;
    private static final String defaultSEName = "ALICE::CERN::EOSALICEO2";
    private static final String defaultseioDaemons = "root://eosaliceo2.cern.ch:1094";
    private static final String defaultEosDir = "/eos/aliceo2/ls2data";
    private static final String defaultLocalFile = "/home/jalien/metadata_tool/statistics.csv";
    private static final int defaultThreads = 4;
    static String server;

    public static void main(String[] args) throws IOException {
        creatorProperties = ConfigUtils.getConfiguration("metadata_tool");

        System.out.println("Storage Element Name: " + creatorProperties.gets("seName", defaultSEName));
        System.out.println("Storage Element seioDaemons: " + creatorProperties.gets("seioDaemons", defaultseioDaemons));
        System.out.println("Eos directory: " + creatorProperties.gets("eosDir", defaultEosDir));
        System.out.println("Local file path: " + creatorProperties.gets("localFile", defaultLocalFile));

        server = creatorProperties.gets("seioDaemons", defaultseioDaemons)
                .substring(creatorProperties.gets("seioDaemons", defaultseioDaemons).lastIndexOf('/') + 1);

        String path = creatorProperties.gets("eosDir", defaultEosDir);
        System.out.println("server: " + server + ", path: " + path);
        BlockingQueue<XrootdFile> dirs = new LinkedBlockingDeque<>();

        Set<XrootdFile> rootDirs = getFirstDirs(path, getFileName(path.substring(path.lastIndexOf('/') + 1), null));
        for (XrootdFile dir : rootDirs) {
            dirs.addAll(getFirstDirs(dir.path, getFileName(dir.getName(), getParentName(dir.path))));
        }

        Thread[] threads = new Thread[creatorProperties.geti("queue.default.threads", defaultThreads)];
        for (int i = 0; i < creatorProperties.geti("queue.default.threads", defaultThreads); i++) {
            threads[i] = new Thread(new ListingThread(dirs));
            threads[i].start();
        }
    }

    static String getParentName(String path) {
        Path parent = Paths.get(path).getParent();
        return parent.getName(parent.getNameCount() - 1).toString();
    }

    static String getFileName(String dirName, String parent) {
        String localFile = creatorProperties.gets("localFile", defaultLocalFile);
        String name = localFile.substring(0, localFile.lastIndexOf('.')) + "-";
        name += (parent  != null) ? (parent + "-" + dirName) : dirName;
        String extension = localFile.substring(localFile.lastIndexOf(".") + 1);
        return String.format("%s.%s", name, extension);
    }

    private static Set<XrootdFile> getFirstDirs(String path, String fileName) throws IOException {
        XrootdListing listing = new XrootdListing(server, path, null);
        Set<XrootdFile> files = listing.getFiles();

        if (!files.isEmpty()) {
            try (FileWriter writer = new FileWriter(fileName, true)) {
                for (XrootdFile f : files) {
                    writer.write(f.path + ", " + Format.size(f.size) + "\n");
                }
            }
        }

        return listing.getDirs();
    }
}
