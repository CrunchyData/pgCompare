##AVAILABLE BUILD OPTIONS -
##      APPVERSION - Variable to set the version label
##      PROGRAM - Name of jar file, pgcompare
##      CONTAINER - prefix and name of the generated container
##      DATE - Date String used as alternate tag for generated containers
##      BASE_REGISTRY - This is the registry to pull the base image from
##      BASE_IMAGE - The base image to use for the final container
##      TARGETARCH - The architecture the resulting image is based on and the binary is compiled for
##      IMAGE_TAG - The tag to be applied to the container

# Makefile for building Docker container with Maven

# Define variables
APPVERSION ?= latest
PROGRAM ?= pgcompare
PGCOMPARE_OPTIONS ?= "--batch 0 --project 2"

CONTAINER ?= brianpace/$(PROGRAM)
DATE ?= $(shell date +%Y%m%d)
BASE_REGISTRY ?= registry.access.redhat.com/ubi8
BASE_IMAGE ?= ubi-minimal
SYSTEMARCH = $(shell uname -m)
MAVEN_VER ?= 3.9.9

ifeq ($(SYSTEMARCH), x86_64)
TARGETARCH ?= amd64
PLATFORM=amd64
else
TARGETARCH ?= arm64
PLATFORM=arm64
JAVA_OPT="-XX:UseSVE=0"
endif

IMAGE_TAG ?= $(APPVERSION)-$(TARGETARCH)
DATE_TAG ?= $(DATE)-$(TARGETARCH)

RM = /bin/rm
CP = /bin/cp
MKDIR = /bin/mkdir
SED = /usr/bin/sed
DOCKERCMD ?= podman

.DEFAULT_GOAL := help

# Default target
all: build docker ##                Build Maven project and Docker container


build: ##              Build the Maven project
	@echo "Running Maven install..."
	mvn clean install


check:  ##              Check pgCompare in Docker container
	@echo "Running test"
	$(DOCKERCMD) run --rm -e PGCOMPARE_OPTIONS="--version" $(CONTAINER):$(IMAGE_TAG)


clean:  ##              Clean up the local Maven repository and target directory
	@echo "Cleaning up Maven and Docker files..."
	mvn clean
	$(DOCKERCMD) rmi  $(CONTAINER):$(IMAGE_TAG) $(CONTAINER):$(DATE_TAG)


docker: set-local docker-common  ##             Generate a BASE_IMAGE container with APPVERSION tag, using matching local cpu architecture

multi-stage-docker: set-multi-stage docker-common ## Generate a BASE_IMAGE container with APPVERSION tag, using matching target cpu architecture

docker-common:  ##      Build the Docker image
	@echo "Building Docker image..."
	$(DOCKERCMD) build -f Dockerfile \
	        --target $(BUILDTYPE) \
    		--build-arg BASE_IMAGE=$(BASE_IMAGE) \
    		--build-arg BASE_REGISTRY=$(BASE_REGISTRY) \
    		--build-arg PLATFORM=$(PLATFORM) \
    		--build-arg TARGETARCH=$(TARGETARCH) \
    		--build-arg VERSION=$(APPVERSION) \
            --build-arg JAVA_OPT=$(JAVA_OPT) \
            --build-arg MAVEN_VERSION=$(MAVEN_VER) \
            --label vendor="Crunchy Data" \
            --label url="https://crunchydata.com" \
            --label release="$(APPVERSION)" \
            --label org.opencontainers.image.vendor="Crunchy Data" \
            -t $(CONTAINER):$(IMAGE_TAG) \
            -t $(CONTAINER):$(DATE_TAG) .
	docker image prune --filter label=stage=pgcomparebuilder -f

run:  ##                Run the Docker container
	@echo "Running Docker container..."
	$(DOCKERCMD) run --rm --network host -e PGCOMPARE_OPTIONS=$(PGCOMPARE_OPTIONS) $(CONTAINER):$(IMAGE_TAG)

set-local:
	$(eval BUILDTYPE = local)

set-multi-stage:
	$(eval BUILDTYPE = multi-stage)

# Show status
status:
	@echo "Current Docker image(s):"
	$(DOCKERCMD) images ${CONTAINER}



install: $(PROGRAM)  ##            This will install the program locally
	mvn clean install
	$(MKDIR) -p $(DESTDIR)/opt/pgcompare/
	$(CP) target/* $(DESTDIR)/opt/pgcompare/


uninstall:  ##          This will uninstall the program from your local system
	$(RM) -r $(DESTDIR)/opt/pgcompare/


help:   ##               Prints this help message
	@echo ""
	@echo ""
	@fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | fgrep -v : | sed -e 's/\\$$//' | sed -e 's/.*##//'
	@echo ""
	@echo "BUILD TARGETS:"
	@fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | fgrep : | sed -e 's/\\$$//' | sed -e 's/:.*##/:/'
	@echo ""
	@echo ""

.PHONY: all build docker multi-stage-docker run clean status
