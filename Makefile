build_spooler:
	javac -cp ./libs/alien-cs_v1.4.2.jar -d ./out_spooler ./src/spooler/*.java
	cp -r config ./out_spooler

build_metadata_tool:
	javac -cp ./libs/alien-cs_v1.4.2.jar -d ./out_metadata_tool ./src/metadata_tool/*.java
	cp -r config ./out_metadata_tool

run_spooler:
	java -cp ./libs/alien-cs_v1.4.2.jar:spooler.jar -DAliEnConfig=../jalien-setup/volume/ spooler.Main

jar_spooler:
	cd out_spooler && jar cvf ../spooler.jar * && cd ..

jar_metadata_tool:
	cd out_metadata_tool && jar cvf ../metadata_tool.jar * && cd ..

deploy_spooler:
	cp ./spooler.jar ../jalien-setup/volume
	cp ./config/spooler.properties ../jalien-setup/volume/config/spooler

deploy_registrator:
	cp ./src/registrator/daqreg.jsp ../tomcat/webapps/ROOT

clean:
	rm -rf ./out_spooler ./out_metadata_tool spooler.jar metadata_tool.jar
