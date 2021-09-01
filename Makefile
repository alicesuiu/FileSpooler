build:
	javac -cp ./libs/alien-cs.jar -d ./out ./src/spooler/*.java
	cp -r config ./out
run:
	java -cp ./libs/alien-cs.jar:spooler.jar -DAliEnConfig=../jalien-setup/volume/ spooler.Main
jar:
	cd out && jar cvf ../spooler.jar * && cd ..
deploy_spooler:
	cp ./spooler.jar ../jalien-setup/volume
	cp ./config/spooler.properties ../jalien-setup/volume/config/spooler
deploy_registrator:
	cp ./src/registrator/daqreg.jsp ../tomcat/webapps/ROOT
clean:
	rm -rf ./out spooler.jar
