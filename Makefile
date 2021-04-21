build:
	javac -cp /root/file_spooler/alien-cs.jar *.java
run:
	java -Ddestination.path=/eos/test/recv_dir \
		 -Dsource.path=/root/file_spooler/send_dir \
		 -Dmd5.enable=true \
		 -Dmax.backoff=10 \
		 -Deos.server.path=root://eos.grid.pub.ro \
		 -Dcatalog.dir.path=/root/file_spooler/catalog_dir \
		 -Dmax.threads=4 \
		 -cp /root/file_spooler/alien-cs.jar:. Main
clean:
	rm -rf *.class
