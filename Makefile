PROJECT_NAME  = mycat
VERSION       = 9.9.9.9
DOCKER        := $(shell which docker)
DOCKER_IMAGE  := docker-registry:5000/maven:3.3-jdk-8

docker_mvn_mycat:
	$(DOCKER) run -v $(shell pwd)/:/opt/code --rm -w /opt/code $(DOCKER_IMAGE) mvn clean install

docker_mvn_ushard:
	$(DOCKER) run -v $(shell pwd)/:/opt/code --rm -w /opt/code $(DOCKER_IMAGE) mvn clean install -f ushard.xml -Dmaven.test.skip=true -Dneed.obfuscate=false

docker_mvn_ushard_obfuscate:
	$(DOCKER) run -v $(shell pwd)/:/opt/code --rm -w /opt/code $(DOCKER_IMAGE) mvn clean install -f ushard.xml -Dmaven.test.skip=true -Dneed.obfuscate=true

upload_mycat:
	curl -T $(shell pwd)/target/*-linux.tar.gz -u admin:ftpadmin ftp://release-ftpd/actiontech-${PROJECT_NAME}/qa/${VERSION}/actiontech-mycat.tar.gz

upload_ushard:
	curl -T $(shell pwd)/target/*-linux.tar.gz -u admin:ftpadmin ftp://release-ftpd/actiontech-${PROJECT_NAME}/qa/${VERSION}/actiontech-ushard-core.tar.gz

upload_ushard_obfuscate:
	curl -T $(shell pwd)/target/*-linux.tar.gz -u admin:ftpadmin ftp://release-ftpd/actiontech-${PROJECT_NAME}/qa/${VERSION}/actiontech-ushard-core-obfuscate.tar.gz
