package metadata_tool;

import alien.io.xrootd.XrootdFile;
import alien.io.xrootd.XrootdListing;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

public class ListingThread implements Runnable {
    private BlockingQueue<XrootdFile> dirs;

    ListingThread(BlockingQueue<XrootdFile> dirs) {
        this.dirs = dirs;
    }

    @Override
    public void run() {
        while(!dirs.isEmpty()) {
            try {
                XrootdFile dir = dirs.take();
                String fileName = Main.getFileName(dir.getName(), Main.getParentName(dir.path));
                try (FileWriter writer = new FileWriter(fileName, true)) {
                    listAll(dir.path, writer);
                } catch (IOException e) {
                    System.out.println("Caught exception while writing into the file." + e.getMessage());
                }
            } catch (InterruptedException e) {
                System.out.println("Thread was interrupted");
            }
        }
    }

    private static void listAll(String path, FileWriter writer) throws IOException {
        XrootdListing listing = new XrootdListing(Main.server, path, null);
        Set<XrootdFile> directories = listing.getDirs();
        Set<XrootdFile> files = listing.getFiles();

        for (XrootdFile f : files) {
            writer.write(f.path + ", " + f.size + "\n");
        }

        if (directories.isEmpty()) {
            return;
        }

        for (XrootdFile dir : directories) {
            listAll(dir.path, writer);
        }
    }
}
