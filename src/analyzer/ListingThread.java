package analyzer;

import alien.config.ConfigUtils;
import alien.io.xrootd.XrootdFile;
import alien.io.xrootd.XrootdListing;
import spooler.Pair;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ListingThread implements Runnable {
    private BlockingQueue<String> dirs;
    private static Logger logger = ConfigUtils.getLogger(ListingThread.class.getCanonicalName());

    ListingThread(BlockingQueue<String> dirs) {
        this.dirs = dirs;
    }

    @Override
    public void run() {
        while(!dirs.isEmpty()) {
            try {
                String dir = dirs.take();
                Pair<Integer, Long> stats =  ListingUtils.scanFiles(dir);
                try (FileWriter writer = new FileWriter(ListingUtils.statRootDirsFileName, true)) {
                    writer.write("path: " + dir + ", size: " + stats.getSecond() + ", files: " + stats.getFirst() + "\n");
                }
            } catch (InterruptedException | IOException e) {
                logger.log(Level.WARNING, "Thread was interrupted");
            }
        }
    }
}
