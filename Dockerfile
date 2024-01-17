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
FROM openjdk:21-ea-slim

MAINTAINER brianpace#

#############################################
# -------------------------------------------
# Make Directories
# -------------------------------------------
#############################################
USER 0

RUN mkdir /opt/conferodc \
    && chown -R 1001:1001 /opt/conferodc

COPY docker/start.sh /opt/conferodc/

RUN chmod 770 /opt/conferodc/start.sh

#############################################
# -------------------------------------------
# Copy in ConferoDC Compiled Application
# -------------------------------------------
#############################################
USER 1000

# Environment variables
# -------------------------------------------------------------
ENV CONFERODC_HOME=/opt/conferodc \
    CONFERODC_CONFIG=/etc/conferodc/confero.properties \
    PATH=/opt/conferodc:$PATH

COPY target/ /opt/conferodc/

CMD ["start.sh"]

WORKDIR "/opt/conferodc"