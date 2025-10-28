# To run in a loop to test all platforms:
#  for T in db2 mariadb mssql mysql oracle snowflake
#  do
#    ./test.sh $T 2>&1 | grep -v "INFO"
#  done

export TEST_PLATFORM=$1
export OUTPUT_BASE=~/app/temp/pgcompare
echo "====================================="
echo Test ${TEST_PLATFORM} to Postgres
echo "====================================="
cd target
rm ${OUTPUT_BASE}/${TEST_PLATFORM}*

export PGCOMPARE_CONFIG=~/app/gitecto/work/pgCompare-Test/pgcompare.${TEST_PLATFORM}.properties

echo "  Discovery:"
export PGCOMPARE_LOG_DESTINATION=${OUTPUT_BASE}/${TEST_PLATFORM}-full-a-discover.log
java -jar pgcompare.jar discover --report ${OUTPUT_BASE}/${TEST_PLATFORM}-full-a-discover.html 2>&1 | grep -v "INFO" | awk '{ print "      " $0 }'

echo "  Compare:"
export PGCOMPARE_LOG_DESTINATION=${OUTPUT_BASE}/${TEST_PLATFORM}-full-b-compare.log
java -jar pgcompare.jar compare --report ${OUTPUT_BASE}/${TEST_PLATFORM}-full-b-compare.html 2>&1 | grep -v "INFO" | awk '{ print "      " $0 }'

echo "  Check:"
export PGCOMPARE_LOG_DESTINATION=${OUTPUT_BASE}/${TEST_PLATFORM}-full-c-check.log
java -jar pgcompare.jar check --report ${OUTPUT_BASE}/${TEST_PLATFORM}-full-c-check.html 2>&1 | grep -v "INFO" | awk '{ print "      " $0 }'

#export PGCOMPARE_LOG_DESTINATION=${OUTPUT_BASE}/${TEST_PLATFORM}-plat-a-discover.log
#export PGCOMPARE_CONFIG=/Users/bpace/app/gitecto/db-projects/pgCompare-Test/pgcompare.${TEST_PLATFORM}.properties
#java -jar pgcompare.jar --discover --table plat
#export PGCOMPARE_LOG_DESTINATION=${OUTPUT_BASE}/${TEST_PLATFORM}-plat-b-compare.log
#java -jar pgcompare.jar --table plat
#export PGCOMPARE_LOG_DESTINATION=${OUTPUT_BASE}/${TEST_PLATFORM}-plat-c-check.log
#java -jar pgcompare.jar --check --table plat

cd -