<div>
  <h1 style="font-size: 70px;text-align: center">pgCompare</h1>
  <h2 style="text-align: center">Data Compare</h2>
</div>
<hr>

[![License](https://img.shields.io/github/license/CrunchyData/postgres-operator)](LICENSE.md)

# Data Compare Made Simple

pgCompare is a Java application designed for use in situations where you are replicating data from different sources and need to validate that post-replication data consistency, including:

- **Data migration from Oracle to Postgres:**  Migrating from Oracle to Postgres? pgCompare can be utilized to compare data between Oracle and Postgres during and/or after the process of data migration.


- **Logical replication between same or different database platforms:** pgCompare exhibits enhanced optimization when comparing data, reducing the overhead on the source and target databases. The comparison tasks can also be delegated to a physical replication target. pgCompare not only identifies rows that appear out-of-sync but also offers the capability to revalidate those rows, proving valuable in the process of verifying data on active systems.


- **Active-Active replication configuration:**  There are inherent data consistency risks associated with any active-active database setup. To meet verification requirements, pgCompare can be employed regularly to compare either all or specific portions of the data.

At a higher level, pgCompare reads a row from a table and generates two hash values. The initial hash is executed on the primary key column(s), while the second hash is computed on the remaining columns. The hash can be calculated by the database or by the application. These hash values are stored in the pgCompare repository. Representing the original values as a hash minimizes the required space in the repository database and decreases network traffic. A parallel process consistently conducts comparisons on sets of data as they are loaded to expedite the comparison process and avoid row-by-row processing. Ultimately, a summary of the comparison results is displayed and stored in the pgCompare repository.

pgCompare is an open-source project maintained by the team at Crunchy Data and made available under the Apache 2.0 licenses for broader use, testing, and feedback.

# Installation

### Requirements
Before initiating the build and installation process, ensure the following prerequisites are met:

1. Java version 21 or higher.
2. Maven 3.9 or higher.
3. Postgres version 15 or higher (to use for the pgCompare Data Compare repository).
4. Necessary JDBC drivers (Postgres and Oracle currently supported).

### Limitations
The following are current limitations of the compare utility:

1. Date/Timestamp only compared with a precision of second (DDMMYYYYHH24MISS).
2. Unsupported data types: blob, long, longraw, bytea.
3. Limitations with data type boolean when performing cross-platform compare. 

### Compile
Once the prerequisites are met, begin by forking the repository and cloning it to your host machine:

```shell
YOUR_GITHUB_UN="<your GitHub username>"
git clone --depth 1 "git@github.com:${YOUR_GITHUB_UN}/pgCompare.git"
cd pgCompare
```

Compile the Java source:

```shell
mvn clean install
```

### Configure Repository Database
pgCompare necessitates a hosted Postgres repository. To configure, connect to a Postgres database and execute the provided pgCompare.sql script in the database directory.  The repository may also be created using the `--init` flag.

```shell
java -jar pgcompare --init
```

# Getting Started

### Defining Table Mapping
The initial step involves defining a set of tables to compare, achieved by inserting rows into the `dc_table` within the pgCompare repository.


dc_table:
- source_schema: Schema/user that owns the table on the source database.
- source_table: Table name on the source database.
- target_schema:  Schema/user that owns the table on the target database.
- target_table: Table name on the target database.
- table_filter:  Specify a valid predicate that would be used in the where clause of a select sql statement.
- parallel_degree:  Data can be compared by splitting up the work among many threads.  The parallel_degree determines the number of threads.  To use parallel threads, the mod_column value must be specified.
- status: Expected values are 'disabled', which is the default, and 'ready'.
- batch_nbr:  Tables can be grouped into batches and compare jobs executed a batch, or grouping of tables.
- mod_column:  Used in conjunction with the parallel_degree.  The work is divided up among the threads using a mod of the specified column.  Therefore, the value entered must be a single column with a numeric data type.
- column_map:  Used to review or override column mapping used by compare functions.  See Column Map section for more details.

Example of loading a row into `dc_table`:


```sql
INSERT INTO dc_table (source_schema, source_table, target_schema, target_table, parallel_degree, status, batch_nbr)
  VALUES ('hr','emp','hr','emp',1,'ready',1);
```

### Create `pgcompare.properties`
Copy the `pgcompare.properties.sample` file to pgcompare.properties and define the repository, source, and target connection parameters.  Refer to the Properties section for more details on the settings.

By default, the application looks for the properties file in the execution directory.  Use the PGCOMPARE_CONFIG environment variable override the default and point to a file in a different location.

### Perform Data Compare
With the table mapping defined, execute the comparison and provide the mandatory batch command line argument:

```shell
java -jar pgcompare --batch=0
```

Using a batch value of 0 will execute the action for all batches.  The batch number may also be specified using the environment variable PGCOMPARE-BATCH.  The default value for batch number is 0 (all batches).

### Debug/Recheck Out-of-Sync Rows
If discrepancies are detected, run the comparison with the 'check' option:

```shell
java -jar pgcompare --batch=0 --check
```
This recheck process is useful when transactions may be in flight during the initial comparison.  The recheck only checks the rows that have been flagged with a discrepancy.  If the rows still do not match, details will be reported.  Otherwise, the rows will be cleared and marked in-sync.

# Column Map
The system will automatically generate a column mapping during the first execution on a table.  This column mapping will be stored in the column_map column of the dc_table repository table. The column mapping is stored in the form of a JSON object.  This mapping can be performed ahead of time or the generated mapping modified as needed.  If a column map is present in column_map, the program will not perform a remap.

To create or overwrite current column mappings stored in column_map colum of dc_table, execute the following:

```shell
java -jar pgcompare --batch=0 --maponly
```

## JSON Mapping Object
Below is a sample of a column mapping.

```json
{
  "columns": [
    {
      "alias": "cola",
      "source": {
        "dataType": "char",
        "nullable": true,
        "dataClass": "char",
        "dataScale": 22,
        "supported": true,
        "columnName": "cola",
        "dataLength": 2,
        "primaryKey": false,
        "dataPrecision": 44,
        "valueExpression": "nvl(trim(col_char_2),' ')"
      },
      "status": "compare",
      "target": {
        "dataType": "bpchar",
        "nullable": false,
        "dataClass": "char",
        "dataScale": 22,
        "supported": true,
        "columnName": "cola",
        "dataLength": 2,
        "primaryKey": false,
        "dataPrecision": 44,
        "valueExpression": "coalesce(col_char_2::text,' ')"
      }
    },
    {
      "alias": "id",
      "source": {
        "dataType": "number",
        "nullable": false,
        "dataClass": "numeric",
        "dataScale": 0,
        "supported": true,
        "columnName": "id",
        "dataLength": 22,
        "primaryKey": true,
        "dataPrecision": 8,
        "valueExpression": "lower(nvl(trim(to_char(id,'0.9999999999EEEE')),' '))"
      },
      "status": "compare",
      "target": {
        "dataType": "int4",
        "nullable": true,
        "dataClass": "numeric",
        "dataScale": 0,
        "supported": true,
        "columnName": "id",
        "dataLength": 32,
        "primaryKey": true,
        "dataPrecision": 32,
        "valueExpression": "coalesce(trim(to_char(id,'0.9999999999EEEE')),' ')"
      }
    }
  ]
}
```

Only Primary Key columns and columns with a status equal to 'compare' will be included in the final data compare.

# Properties
Properties are categorized into four sections: system, repository, source, and target. Each section has specific properties, as described in detail in the documentation.  The properties can be specified via a configuration file, environment variables or a combination of both.  To use environment variables, the environment variable will be the name of hte property in upper case prefixed with "PGCOMPARE-".  For example, batch-fetch-size can be set by using the environment variable PGCOMPARE-BATCH-FETCH-SIZE.

### System
- batch-fetch-size: Sets the fetch size for retrieving rows from the source or target database.
- batch-commit-size:  The commit size controls the array size and number of rows concurrently inserted into the dc_source/dc_target staging tables.
- batch-progress-report-size:  Defines the number of rows used in mod to report progress.
- loader-threads: Sets the number of threads to load data into the temporary tables. Default is 4.  Set to 0 to disable loader threads.
- message-queue-size:  Size of message queue used by loader threads (nbr messages).  Default is 100.
- number-cast: Defines how numbers are cast for hash function (notation|standard).  Default is notation (for scientific notation).
- observer-throttle:  Set to true or false, instructs the loader threads to pause and wait for the observer thread to catch up before continuing to load more data into the staging tables.
- observer-throttle-size:  Number of rows loaded before the loader thread will sleep and wait for clearance from the observer thread.
- observer-vacuum:  Set to true or false, instructs the observer whether to perform a vacuum on the staging tables during checkpoints.
- stage-table-parallel: Sets the number of parallel workers for the temporary staging tables.  Default is 0.

### Repository
- repo-dbname:  Repository database name.
- repo-host: Host name of server hosting the Postgres repository database.
- repo-password:  Postgres database user password.
- repo-port:  Repository Postgres instance port.
- repo-schema:  Name of schema that owns the repository tables.
- repo-sslmode: Set the SSL mode to use for the database connection (disable|prefer|require)
- repo-user:  Postgres database username.

### Source
- source-database-hash: True or false, instructs the application where the hash should be computed (on the database or by the application).
- source-dbname:  Database or service name.
- source-host:  Database server name.
- source-name:  User defined name for the source.
- source-password:  Database password.
- source-port:  Database port.
- source-sslmode: Set the SSL mode to use for the database connection (disable|prefer|require)
- source-type:  Database type: oracle, postgres
- source-user:   Database username.

### Target
- target-database-hash: True or false, instructs the application where the hash should be computed (on the database or by the application).
- target-dbname:  Database or service name.
- target-host:  Database server name.
- target-name:  User defined name for the target.
- target-password:  Database password.
- target-port:  Database port.
- target-sslmode: Set the SSL mode to use for the database connection (disable|prefer|require)
- target-type:  Database type: oracle, postgres
- target-user:  Database username.

# Data Compare
pgCompare stores a hash representation of primary key columns and other table columns, reducing row size and storage demands. The utility optimizes network traffic and speeds up the process by using hash functions when comparing similar platforms.

## Hash Options
By default, the data is normalized and the hash performed by the Java code.  For supported platforms (Oracle 12.1 or higher and Postgres 11 or higher), the program will leverage database functions to perform the hash.  Allowing the database to perform the hash does increase CPU load on the hosting server, but reduces network traffic, increases speed, and reduces memory requirements of pgCompare.

## Processes
Each comparison involves at least three threads: one for the observer and two for the source and target loader processes. By specifying a mod_column in the dc_tables and increasing parallel_degree, the number of threads can be increased to speed up comparison. Tuning between batch sizes, commit rates, and parallel degree is essential for optimal performance.

## Viewing Results
A summary is printed at the end of each run.  To view results at a later time, the following SQL may be used in the repository database.

#### Results from Last Run
```sql
WITH mr AS (SELECT max(rid) rid FROM dc_result)
SELECT compare_dt, table_name, status, source_cnt total_cnt, equal_cnt, not_equal_cnt, missing_source_cnt+missing_target_cnt missing_cnt
FROM dc_result r
     JOIN mr ON (mr.rid=r.rid)
ORDER BY table_name;
```

#### Out of Sync Rows
```sql
SELECT coalesce(s.table_name,t.table_name) table_name,
	   coalesce(s.batch_nbr, t.batch_nbr) batch_nbr,
       coalesce(s.thread_nbr,t.thread_nbr) thread_nbr,
       CASE WHEN s.compare_result='n' THEN 'out-of-sync'
            WHEN s.compare_result='m' THEN 'missing target'
            WHEN t.compare_result='m' THEN 'missing source'
            ELSE 'unknown'
       END compare_result,
       coalesce(s.pk,t.pk) primary_key       
FROM dc_source s
     CROSS JOIN dc_target t;
```

# Design Principles
The following list outlines some principles that drives the design of this solution:
- Avoid the need to create any database object in the source or target host.
- Compare rows in batches instead of row by row or column by column approach.

# Performance

## Repository Database
The repository database will have a measurable amount of load during the compare process.  To ensure optimal performance, consider the following recommendations:

- Database Host:
  - Minimal vCPUs for database host should be:  2 + (max(parallel_degree)*2)
  - Memory should be a minimal of 8GB
- Minimal Postgres parameter settings:
  - shared_buffers = 2048MB
  - work_mem = 256MB
  - maintenance_work_mem = 512MB
  - max_parallel_workers = 16 (Note: Do not exceed 3 times vCPU count)

## Compute Resources for Compare
pgCompare requires the execution host to allocate 3 - 12 threads per degree of parallelism (can be higher based on loader-threads setting), with each thread utilizing approximately 400 MB of memory.


pgCompare project source code is available subject to the [Apache 2.0 license](LICENSE.md).
