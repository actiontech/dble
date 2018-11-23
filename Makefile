PROJECT_NAME  = dble
DOCKER        := $(shell which docker)

default:
	$(DOCKER) run -v $(shell pwd)/:/volume/git  --rm -w /volume/git jamesdbloom/docker-java8-maven mvn clean install

