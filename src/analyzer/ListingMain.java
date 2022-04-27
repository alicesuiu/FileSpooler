package analyzer;

import alien.config.ConfigUtils;
import alien.io.xrootd.XrootdFile;
import lazyj.ExtProperties;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class ListingMain {
    static ExtProperties listingProperties;
    private static final String defaultSEName = "ALICE::CERN::EOSALICEO2";
    private static final String defaultseioDaemons = "root://eosaliceo2.cern.ch:1094";
    private static final String defaultEosDir = "/eos/aliceo2/ls2data";
    static final String defaultLocalFile = "/home/jalien/metadata_tool/statistics.csv";
    private static final int defaultThreads = 4;
    static String server;

    public static void main(String[] args) throws IOException {
        listingProperties = ConfigUtils.getConfiguration("listing");

        System.out.println("Storage Element Name: " + listingProperties.gets("seName", defaultSEName));
        System.out.println("Storage Element seioDaemons: " + listingProperties.gets("seioDaemons", defaultseioDaemons));
        System.out.println("Eos directory: " + listingProperties.gets("eosDir", defaultEosDir));
        System.out.println("Local file path: " + listingProperties.gets("localFile", defaultLocalFile));

        server = listingProperties.gets("seioDaemons", defaultseioDaemons)
                .substring(listingProperties.gets("seioDaemons", defaultseioDaemons).lastIndexOf('/') + 1);

        String path = listingProperties.gets("eosDir", defaultEosDir);
        System.out.println("server: " + server + ", path: " + path);
        BlockingQueue<XrootdFile> dirs = new LinkedBlockingDeque<>();

        Set<XrootdFile> rootDirs = ListingUtils.getFirstDirs(path, ListingUtils.getFileName(path.substring(path.lastIndexOf('/') + 1), null));
        for (XrootdFile dir : rootDirs) {
            dirs.addAll(ListingUtils.getFirstDirs(dir.path, ListingUtils.getFileName(dir.getName(), ListingUtils.getParentName(dir.path))));
        }

        Thread[] threads = new Thread[listingProperties.geti("queue.default.threads", defaultThreads)];
        for (int i = 0; i < listingProperties.geti("queue.default.threads", defaultThreads); i++) {
            threads[i] = new Thread(new ListingThread(dirs));
            threads[i].start();
        }
    }


}
