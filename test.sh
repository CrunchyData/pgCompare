export TEST_PLATFORM=$1
echo Test ${TEST_PLATFORM} to Postgres
cd target
export PGCOMPARE_LOG_DESTINATION=/app/temp/pgcompare-${TEST_PLATFORM}-discover.log
export PGCOMPARE_CONFIG=/Users/bpace/app/gitecto/db-projects/pgCompare-Test/pgcompare.${TEST_PLATFORM}.properties
java -jar pgcompare.jar --discover
export PGCOMPARE_LOG_DESTINATION=/app/temp/pgcompare-${TEST_PLATFORM}.log
java -jar pgcompare.jar
export PGCOMPARE_LOG_DESTINATION=/app/temp/pgcompare-${TEST_PLATFORM}-check.log
java -jar pgcompare.jar --check
cd -