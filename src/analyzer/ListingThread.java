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
                Pair<Integer, Long> stats =  scanFiles(dir);
                try (FileWriter writer = new FileWriter(ListingUtils.statRootDirsFileName, true)) {
                    writer.write("path: " + dir + ", size: " + stats.getSecond() + ", files: " + stats.getFirst() + "\n");
                }
            } catch (InterruptedException | IOException e) {
                logger.log(Level.WARNING, "Thread was interrupted");
            }
        }
    }

    private Pair<Integer, Long> scanFiles(String path) {
        XrootdListing listing;
        int nr_files = 0;
        long nr_bytes = 0L;
        try {
            listing = new XrootdListing(ListingUtils.server, path);
            Set<XrootdFile> directories = listing.getDirs();
            Set<XrootdFile> listFiles = listing.getFiles();

            try (FileWriter writer = new FileWriter(ListingUtils.statFileName, true)) {
                for (XrootdFile file : listFiles) {
                    nr_files += 1;
                    nr_bytes += file.size;

                    writer.write(file.path + ", " + file.size + "\n");
                }
            } catch (IOException e) {
                logger.log(Level.WARNING, "Cannot write to the file: " + ListingUtils.statFileName);
            }

            for (XrootdFile dir : directories) {
                Pair<Integer, Long> stats =  scanFiles(dir.path);
                nr_files += stats.getFirst();
                nr_bytes += stats.getSecond();
            }
            return new Pair<>(nr_files, nr_bytes);
        } catch (IOException e) {
           //todo
        }

        return new Pair<>(0, 0L);
    }
}
