#!/bin/bash

# Check number of parameters
if [[ "$#" -lt 3 ]]; then
	echo "[Usage]: ./generate_files.sh <number_of_files> <file_size [1G]> <path_to_files>"
	exit 1
fi

# Check first parameters
if ! [[ $1 =~ ^[0-9]+$ ]]; then
	echo "First parameter is not a valid number!"
	echo "[Usage]: ./generate_files.sh <val> p2 p3, val is integer, gt 0"
	exit 1
fi

# Check second parameter
if ! [[ $2 =~ ^[0-9]+(.[0-9]+)?[KMG]?$ ]]; then
	echo "Second parameter is not a valid number!"
	echo "[Usage]: ./generate_files.sh p1 <val> p3, val is numerical, gt 0"
	exit 1
fi

# Check third parameter
if ! [[ -d $3 ]]; then
	echo "Third parameter is not a valid directory!"
	exit 1
fi

# Generating files
for ((i = 0; i < $1; i++)); do
	filename=file_$(uuidgen)
	echo $filename
	dd if=/dev/urandom of=$3/$filename.root bs=$2 count=1024 status=none
done