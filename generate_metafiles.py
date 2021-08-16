import sys
import os
import string
import random
import uuid

directory = sys.argv[1]
dest = sys.argv[2]

for filename in os.listdir(directory):
	metadata = {}
	if filename.endswith(".root"):
		print(filename)
		abspath = os.path.abspath(directory + "/" + filename)
		metadata['lurl'] = abspath
		metadata['surl'] = os.path.abspath(dest + filename)
		metadata['size'] = os.stat(abspath).st_size
		metadata['ctime'] = long(os.stat(abspath).st_ctime)
		metadata['run'] = ''.join(random.choice(string.digits) for _ in range(9))
		metadata['guid'] = uuid.uuid4()
		metadata['meta:accPeriod'] = 'LHC18r_PHOS'
		metadata['type'] = "raw"

		output = directory + filename.replace('.root', '.done')
		print(output)
		metafile = open(output, "w")
		for key, value in metadata.items():
			metafile.write(key + ": " + str(value) + "\n")
		metafile.close()

