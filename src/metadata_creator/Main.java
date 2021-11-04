package metadata_creator;

import alien.config.ConfigUtils;
import alien.io.xrootd.XrootdFile;
import alien.io.xrootd.XrootdListing;
import lazyj.ExtProperties;

import java.io.IOException;
import java.util.Set;

public class Main {
    private static ExtProperties creatorProperties;
    private static final String defaultSEName = "ALICE::CERN::EOSALICEO2";
    private static final String defaultseioDaemons = "root://eosaliceo2.cern.ch:1094";
    private static final String defaultEosDir = "/eos/aliceo2/ls2data";
    private static String server;

    public static void main(String[] args) throws IOException {
        creatorProperties = ConfigUtils.getConfiguration("metadata_creator");

        System.out.println("Storage Element Name: " + creatorProperties.gets("seName", defaultSEName));
        System.out.println("Storage Element seioDaemons: " + creatorProperties.gets("seioDaemons", defaultseioDaemons));
        System.out.println("Eos directory: " + creatorProperties.gets("eosDir", defaultEosDir));

        server = creatorProperties.gets("seioDaemons", defaultseioDaemons)
                .substring(creatorProperties.gets("seioDaemons", defaultseioDaemons).lastIndexOf('/') + 1);

        String path = creatorProperties.gets("eosDir", defaultEosDir);

        System.out.println("server: " + server + ", path: " + path);

        Set<XrootdFile> allFiles = listAll(path);
        System.out.println("Size: " + allFiles.size());
        //allFiles.forEach(file -> {System.out.println(file.path);});
    }

    private static Set<XrootdFile> listAll(String path) throws IOException {
        XrootdListing listing = new XrootdListing(server, path, null);
        Set<XrootdFile> directories = listing.getDirs();
        Set<XrootdFile> files = listing.getFiles();

        if (directories.isEmpty()) {
            return listing.getFiles();
        }

        for (XrootdFile dir : directories) {
            files.addAll(listAll(dir.path));
        }

        return files;
    }
}
