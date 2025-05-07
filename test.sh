# To run in a loop to test all platforms:
#  for T in db2 mariadb mssql mysql oracle
#  do
#    ./test.sh $T
#  done

export TEST_PLATFORM=$1
echo Test ${TEST_PLATFORM} to Postgres
cd target
export PGCOMPARE_LOG_DESTINATION=/app/temp/pgcompare/${TEST_PLATFORM}-full-a-discover.log
export PGCOMPARE_CONFIG=/Users/bpace/app/gitecto/db-projects/pgCompare-Test/pgcompare.${TEST_PLATFORM}.properties
java -jar pgcompare.jar --discover
export PGCOMPARE_LOG_DESTINATION=/app/temp/pgcompare/${TEST_PLATFORM}-full-b-compare.log
java -jar pgcompare.jar
export PGCOMPARE_LOG_DESTINATION=/app/temp/pgcompare/${TEST_PLATFORM}-full-c-check.log
java -jar pgcompare.jar --check


export PGCOMPARE_LOG_DESTINATION=/app/temp/pgcompare/${TEST_PLATFORM}-plat-a-discover.log
export PGCOMPARE_CONFIG=/Users/bpace/app/gitecto/db-projects/pgCompare-Test/pgcompare.${TEST_PLATFORM}.properties
java -jar pgcompare.jar --discover --table plat
export PGCOMPARE_LOG_DESTINATION=/app/temp/pgcompare/${TEST_PLATFORM}-plat-b-compare.log
java -jar pgcompare.jar --table plat
export PGCOMPARE_LOG_DESTINATION=/app/temp/pgcompare/${TEST_PLATFORM}-plat-c-check.log
java -jar pgcompare.jar --check --table plat

cd -