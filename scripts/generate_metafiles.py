import sys
import os
import string
import random

dataDir = sys.argv[1]
metadataDir = sys.argv[2]

for filename in os.listdir(dataDir):
	metadata = {}
	if filename.endswith(".root"):
		abspath = os.path.abspath(dataDir + "/" + filename)
		metadata['lurl'] = abspath
		metadata['LHCPeriod'] = 'LHC21r_PHOS'
		metadata['run'] = ''.join(random.choice(string.digits) for _ in range(9))

		output = metadataDir + filename.replace('.root', '.done')
		metafile = open(output, "w")
		for key, value in metadata.items():
			metafile.write(key + ": " + str(value) + "\n")
		metafile.close()

