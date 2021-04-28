build:
	javac -cp /root/file_spooler/alien-cs.jar *.java
run:
	java -cp /root/file_spooler/alien-cs.jar:. Main
clean:
	rm -rf *.class
