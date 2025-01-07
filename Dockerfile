# REQUIRED FILES TO BUILD THIS IMAGE
# ----------------------------------
# (1) target/*
#     See the main README.md file for instructions on compiling.  The compiled version is placed
#     into the target directory by default.
#
# HOW TO BUILD THIS IMAGE
# -----------------------
# Compile code:
#      $ mvn install
# Build Docker Image:
#      $ docker build -t {tag} .
#
# Pull base image
# ---------------
ARG VERSION=v0.3.2
ARG PLATFORM=arm64
ARG BASE_REGISTRY=registry.redhat.io/ubi8
ARG BASE_IMAGE=ubi-minimal

## Local Platform Build
FROM --platform=${PLATFORM} ${BASE_REGISTRY}/${BASE_IMAGE} as local

RUN microdnf install java-21-openjdk -y

USER 0

RUN mkdir /opt/pgcompare \
    && chown -R 1001:1001 /opt/pgcompare

COPY docker/start.sh /opt/pgcompare/

COPY docker/pgcompare.properties /etc/pgcompare/

COPY target/* /opt/pgcompare/

RUN chmod 770 /opt/pgcompare/start.sh

USER 1001

ENV PGCOMPARE_HOME=/opt/pgcompare \
    PGCOMPARE_CONFIG=/etc/pgcompare/pgcompare.properties \
    PATH=/opt/pgcompare:$PATH \
    _JAVA_OPTIONS="-XX:UseSVE=0"

COPY target/ /opt/pgcompare/

CMD ["start.sh"]

WORKDIR "/opt/pgcompare"


## Target Platform Build
FROM --platform=${TARGETARCH} ${BASE_REGISTRY}/${BASE_IMAGE} as multi-stage

RUN microdnf install java-21-openjdk -y

USER 0

RUN mkdir /opt/pgcompare \
    && chown -R 1001:1001 /opt/pgcompare

COPY docker/start.sh /opt/pgcompare/

COPY docker/pgcompare.properties /etc/pgcompare/

COPY target/* /opt/pgcompare/

RUN chmod 770 /opt/pgcompare/start.sh

USER 1001

ENV PGCOMPARE_HOME=/opt/pgcompare \
    PGCOMPARE_CONFIG=/etc/pgcompare/pgcompare.properties \
    PATH=/opt/pgcompare:$PATH \
    _JAVA_OPTIONS="-XX:UseSVE=0"

COPY target/ /opt/pgcompare/

CMD ["start.sh"]

WORKDIR "/opt/pgcompare"
