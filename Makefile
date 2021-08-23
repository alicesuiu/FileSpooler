build:
	javac -cp ./libs/alien-cs.jar -d ./out ./src/spooler/*.java
run:
	java -cp ./libs/alien-cs.jar:./out:. spooler.Main
clean:
	rm -rf ./out/*
