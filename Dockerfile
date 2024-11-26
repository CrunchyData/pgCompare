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

RUN mkdir /opt/pgcompare \
    && chown -R 1001:1001 /opt/pgcompare

COPY docker/start.sh /opt/pgcompare/

COPY target/* /opt/pgcompare/

RUN chmod 770 /opt/pgcompare/start.sh

#############################################
# -------------------------------------------
# Copy in pgCompare Compiled Application
# -------------------------------------------
#############################################
USER 1001

# Environment variables
# -------------------------------------------------------------
ENV PGCOMPARE_HOME=/opt/pgcompare \
    PGCOMPARE_CONFIG=/etc/pgcompare/pgcompare.properties \
    PATH=/opt/pgcompare:$PATH

COPY target/ /opt/pgcompare/

CMD ["start.sh"]

WORKDIR "/opt/pgcompare"