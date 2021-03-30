build:
	javac *.java
run:
	java -Ddestination.path=/eos/test/recv_dir -Dsource.path=/root/file_spooler/send_dir -Dmd5.enable=true -Dmax.backoff=10 -Deos.server.path=root://eos.grid.pub.ro Main
clean:
	rm -rf *.class
