package analyzer;

import alien.io.xrootd.XrootdFile;
import alien.io.xrootd.XrootdListing;
import alien.site.supercomputing.titan.Pair;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

public class ListingThread implements Runnable {
    private String server = Main.listingProperties.gets("seioDaemons", Main.defaultseioDaemons)
            .substring(Main.listingProperties.gets("seioDaemons",
                    Main.defaultseioDaemons).lastIndexOf('/') + 1);
    private BlockingQueue<String> dirs;

    ListingThread(BlockingQueue<String> dirs) {
        this.dirs = dirs;
    }

    @Override
    public void run() {
        while(!dirs.isEmpty()) {
            try {
                String dir = dirs.take();
                Pair<Integer, Long> stats =  scanFiles(dir);
                System.out.println("Dir path: " + dir + " size: " + stats.getSecond() + " files: " + stats.getFirst());
            } catch (InterruptedException | IOException e) {
                System.out.println("Thread was interrupted");
            }
        }
    }

    private Pair<Integer, Long> scanFiles(String path) throws IOException {
        XrootdListing listing = new XrootdListing(server, path);
        Set<XrootdFile> directories = listing.getDirs();
        Set<XrootdFile> listFiles = listing.getFiles();
        int nr_files = 0;
        long nr_bytes = 0L;

        for (XrootdFile file : listFiles) {
            nr_files += 1;
            nr_bytes += file.size;
        }

        for (XrootdFile dir : directories) {
            Pair<Integer, Long> stats =  scanFiles(dir.path);
            nr_files += stats.getFirst();
            nr_bytes += stats.getSecond();
        }

        return new Pair<>(nr_files, nr_bytes);
    }
}
