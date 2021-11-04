build_spooler:
	javac -cp ./libs/alien-cs_v1.4.2.jar -d ./out_spooler ./src/spooler/*.java
	cp -r config ./out_spooler

build_metadata_creator:
	javac -cp ./libs/alien-cs_v1.4.2.jar -d ./out_metadata_creator ./src/metadata_creator/*.java
	cp -r config ./out_metadata_creator

run_spooler:
	java -cp ./libs/alien-cs_v1.4.2.jar:spooler.jar -DAliEnConfig=../jalien-setup/volume/ spooler.Main

jar_spooler:
	cd out_spooler && jar cvf ../spooler.jar * && cd ..

jar_metadata_creator:
	cd out_metadata_creator && jar cvf ../metadata_creator.jar * && cd ..

deploy_spooler:
	cp ./spooler.jar ../jalien-setup/volume
	cp ./config/spooler.properties ../jalien-setup/volume/config/spooler

deploy_registrator:
	cp ./src/registrator/daqreg.jsp ../tomcat/webapps/ROOT

clean:
	rm -rf ./out_spooler ./out_metadata_creator spooler.jar metadata_creator.jar
