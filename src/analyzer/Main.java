package analyzer;

import alien.config.ConfigUtils;
import alien.io.xrootd.XrootdFile;
import lazyj.ExtProperties;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class Main {
    static ExtProperties listingProperties;
    private static final String defaultSEName = "ALICE::CERN::EOSALICEO2";
    static final String defaultseioDaemons = "root://eosaliceo2.cern.ch:1094";
    private static final String defaultListingDirs = "/data/epn2eos_tool/listingDirs";
    private static final int defaultThreads = 4;
    private static BlockingQueue<String> dirs = new LinkedBlockingDeque<>();

    public static void main(String[] args) throws IOException {
        File listingDirs;
        listingProperties = ConfigUtils.getConfiguration("listing");

        if (listingProperties == null) {
            System.out.println("Cannot find listing config file");
            return;
        }

        System.out.println("Storage Element Name: " + listingProperties.gets("seName", defaultSEName));
        System.out.println("Storage Element seioDaemons: " + listingProperties.gets("seioDaemons", defaultseioDaemons));
        System.out.println("Listing Dirs Path: " + listingProperties.gets("listingDirs", defaultListingDirs));
        System.out.println("Number of listing threads: " + listingProperties.geti("queue.default.threads", defaultThreads));

        listingDirs = new File(listingProperties.gets("listingDirs", defaultListingDirs));
        if (!listingDirs.exists() || listingDirs.length() == 0) {
            System.out.println("The file that contains the list of dirs to be processed does not exist or is empty");
            return;
        }

        try(BufferedReader reader = new BufferedReader(new FileReader(listingProperties.gets("listingDirs", defaultListingDirs)))) {
            String path;
            while ((path = reader.readLine()) != null) {
                dirs.add(path.trim());
            }
        } catch (IOException e) {
            System.out.println("Caught exception while trying to read from file "
                    + listingProperties.gets("listingDirs", defaultListingDirs));
        }

        Thread[] threads = new Thread[listingProperties.geti("queue.default.threads", defaultThreads)];
        for (int i = 0; i < listingProperties.geti("queue.default.threads", defaultThreads); i++) {
            threads[i] = new Thread(new ListingThread(dirs));
            threads[i].start();
        }
    }
}
