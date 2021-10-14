import os
import sys
import random

def main():
    # sys.argv[0] - script name
    # sys.argv[1] - path where to store the metadata files

    if len(sys.argv) < 2:
        print("[Usage] python simulate_epn2eos.py metadataDir")
        return -1

    metadataDir = sys.argv[1]

    dataDirs = [
        "/data/epn2eos_tool/dataDir/0/",
        "/data/epn2eos_tool/dataDir/1/",
        "/data/epn2eos_tool/dataDir/2/",
        "/data/epn2eos_tool/dataDir/3/",
        "/data/epn2eos_tool/dataDir/4/"
    ]
    sizes = [1048576, 2097152, 3145728] #, 4194304, 5242880]

    # ./spooler_scripts/generate_files.sh 1 1048576 /data/epn2eos_tool/epn2eos/
    nrFiles = 1
    dataDirIndex = random.randint(0, 4)
    sizeIndex = 0

    print('nrFiles: {}, dataDir: {}, size: {}'.format(str(nrFiles), dataDirs[dataDirIndex], str(sizes[sizeIndex])))

    generateFiles = "/home/jalien/spooler_scripts/generate_files.sh " + str(nrFiles) + " " + str(sizes[sizeIndex]) + " " + dataDirs[dataDirIndex]
    os.system(generateFiles)

    # python3 ./spooler_scripts/generate_metafiles.py /data/epn2eos_tool/dataDir/1 /data/epn2eos_tool/epn2eos/
    generateMeta = "python3 /home/jalien/spooler_scripts/generate_metafiles.py " + dataDirs[dataDirIndex] + " " + metadataDir
    os.system(generateMeta)


if __name__ == "__main__":
    main()
