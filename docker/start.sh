#!/bin/bash

########### SIGINT handler ############
function _int() {
   echo "Stopping container."
   echo "SIGINT received, shutting down!"
}

########### SIGTERM handler ############
function _term() {
   echo "Stopping container."
   echo "SIGTERM received, shutting down!"
}

########### SIGKILL handler ############
function _kill() {
   echo "SIGKILL received, shutting down!"
}

###################################
############# MAIN ################
###################################

if [ "$PGCOMPARE_OPTIONS" == "" ];
then
   export PGCOMPARE_OPTIONS="--batch 0"
fi

java -jar /opt/pgcompare/pgcompare.jar $PGCOMPARE_OPTIONS