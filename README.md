<div>
  <h1 style="font-size: 70px;text-align: center">pgCompare</h1>
  <h2 style="text-align: center">Data Compare</h2>
</div>
<hr>

[![License](https://img.shields.io/github/license/CrunchyData/postgres-operator)](LICENSE.md)

# Data Compare Made Simple

**pgCompare** is a Java-based tool for validating data consistency after replication or migration between databases. It's designed for scenarios like:


- **Data migration from Oracle/DB2/MariaDB/MySQL/MSSQL to Postgres:**  Compare data during or post-migration.

- **Logical replication between same or different database platforms:** Validate data across platforms while minimizing database overhead.

- **Active-Active replication configuration:**  Regularly verify data consistency to mitigate risks.

pgCompare uses hashing to compare table data efficiently. Hash values for primary keys and remaining columns are stored in a repository, reducing storage and network demands. Comparisons are processed in parallel, improving performance.

This open-source project is maintained by **Crunchy Data** under the **Apache 2.0 License** and is made available for broader use, testing, and feedback.

# Features

- Supports Oracle, PostgreSQL, DB2, MariaDB, MySQL, and MSSQL.
- Efficient parallel comparisons using hashing.
- Handles batch processing for performance tuning.
- Stores configurations for multiple comparison projects in a central repository.

# Installation

## Requirements

Before initiating the build and installation process, ensure the following prerequisites are met:

1. **Java** 21 or later.
2. **Maven** 3.9 or later.
3. **Postgres** 15 or later (for the repository).
4. Supported JDBC drivers (Snowflake, DB2, Postgres, MySQL, MSSQL and Oracle currently supported).
5. Direct Postgres connections (e.g., no pgBouncer).

## Limitations

- Date/Timestamps compared only to the second (format: DDMMYYYYHH24MISS).
- Unsupported data types: blob, long, longraw, bytea.
- Cross-platform comparison limitations with boolean type.
- Low precission types (float, real) cannot be compared to high precission types (double).
- All low precission types are cast using a scale of 3 (1 for Snowflake).  If a higher scale is required consider using the map-expression override option.
- Different databases cast float to different values.  Use float-cast option to switch between char and notation (scientific notation) if there are compare problems with float data types.

# Getting Started

## 1. Fork the repository

## 2. Clone and Build

```shell
git clone --depth 1 git@github.com:<your-github-username>/pgCompare.git
cd pgCompare
mvn clean install
```

## 3. Configure Properties

Copy `pgcompare.properties.sample` to `pgcompare.properties` and update the connection parameters for your repository, source, and target databases.
By default, the application looks for the properties file in the execution directory. Use `PGCOMPARE_CONFIG` environment variable to specify a custom properties file location.

At a minimal the `repo-xxxxx` parameters are required in the properties file (or specified by environment parameters).  Besides the properties file and environment variables, another alternative is to store the property settings in the `dc_project` table.  Settings can be stored in the `project_config` column in JSON format ({"parameter": "value"}).  Certain system parameters like log-destination can only be specified via the properties file or environment variables.

## 4. Initialize Repository

Run the script or use the command below to set up the PostgreSQL repository:

```shell
java -jar pgcompare.jar init
```

## 5. Discover Tables

Discover and map tables in specified schemas:

```shell
java -jar pgcompare.jar discover
```

# Usage

## Command Line

```shell
java -jar pgcompare.jar <action> <options>
```

Actions:
- **check**:  Recompare the out of sync rows from previous compare
- **compare**:  Perform database compare
- **copy-table**: Copy pgCompare metadata for table.  Must specify table alias to copy using --table option
- **discover**:  Disocver tables and columns
- **init**: Initialize the repository database

Options:

   -b|--batch {batch nbr}

   -p|--project Project ID

   -r|--report {file} Create html report of compare

   -t|--table {target table}

   --help

## Define Table Mapping

1. Automatic Discovery

    Discover and map tables in specified schemas:

    ```shell
    java -jar pgcompare.jar discover
    ```

2. Manual Registration 

    Insert mappings into `dc_table`, `dc_table_map`, `dc_table_column`, and `dc_table_column_map` tables in the repository.

## Run Data Comparison

```shell
java -jar pgcompare.jar compare --batch 0
```

Batch 0 processes all data. Use `PGCOMPARE-BATCH` or specify the batch number using the `--batch` argument to specify a batch number.

## Recheck Discrepancies

Revalidate flagged rows:

```shell
java -jar pgcompare.jar check --batch 0
```

# Upgrading

## Version 0.5.0 Enhacements

- Snowflake Support - Full integration for Snowflake as source/target
- SQL Fix Generation - Automatic generation of INSERT/UPDATE/DELETE statements (Preview, limited ability)
- Web UI - Modern Next.js-based interface (preview)
- Performance Improvements
- Bug Fixes

**Note:** Drop and recreate the repository to upgrade to 0.5.0.

For more details review the [v0.5.0 Release Noes](RELEASE_NOTES_v0.5.0.md)

## Version 0.4.0 Enhancements

- Improved casting of low precision data types.
- Added html report generation.
- Refactored code for efficiency.
- Modified arguments and added 'verb' clause to command line.
  
**Note:** Drop and recreate the repository to upgrade to 0.4.0.

## Version 0.3.0 Enhancements

- DB2 support.
- Case-sensitive table/column name handling.
- New project configurations for easier management.

**Note:** Drop and recreate the repository to upgrade to 0.3.0.

# Advanced Configuration

## Properties

Define properties via a file, environment variables, or the `dc_project` table. Environment variables override file settings and must be prefixed with `PGCOMPARE_`.

Examples:
- File: `batch-fetch-size=2000`
- Env: `PGCOMPARE_BATCH_FETCH_SIZE=2000`

## Tuning Performance

- **Batch size:** Adjust `batch-fetch-size` and `batch-commit-size` for memory efficiency.
- **Threads:** Use loader-threads (default: 4) for parallel processing.
- **Observer throttle:** Enable to prevent overloading temporary tables (observer-throttle=true).
- **Java Heap Size:** For larger datasets, there may be a need to increase the Java Heap size.  Use the options `-Xms` and `-Xmx` when executing pgCompare (`java -Xms512m -Xmx2g -jar pgcompare.jar`). 

## Repository Recommendations

- Minimal requirements: 2 vCPUs, 8 GB RAM.
- PostgreSQL settings:
  - shared_buffers=2048MB
  - work_mem=256MB
  - max_parallel_workers=16

## Projects

Projects allow for the repository to maintain different mappings for different compare objectives.  This allows a central pgCompare repository to be used for multiple compare projects.  Each table has a `pid` column which is the project id.  If no project is specified, the default project (pid = 1) is used.

# Viewing Results

## Summary from Last Run

```sql
WITH mr AS (SELECT max(rid) rid FROM dc_result)
SELECT compare_start, table_name, status, equal_cnt+not_equal_cnt+missing_source_cnt+missing_target_cnt  AS total_cnt,
       equal_cnt, not_equal_cnt, missing_source_cnt + missing_target_cnt AS missing_cnt
FROM dc_result r
         JOIN mr ON (mr.rid = r.rid)
ORDER BY table_name;
```

## Out-of-Sync Rows

```sql
SELECT COALESCE(s.table_name, t.table_name) AS table_name,
       CASE
           WHEN s.compare_result = 'n' THEN 'out-of-sync'
           WHEN s.compare_result = 'm' THEN 'missing target'
           WHEN t.compare_result = 'm' THEN 'missing source'
           END AS compare_result,
       COALESCE(s.pk, t.pk) AS primary_key
FROM dc_source s
         FULL OUTER JOIN dc_target t ON s.pk = t.pk and s.tid=t.tid;
```

# Reference

## Properties

Properties are categorized into four sections: system, repository, source, and target. Each section has specific properties, as described in detail in the documentation.  The properties can be specified via a configuration file, environment variables or a combination of both.  To use environment variables, the environment variable will be the name of the property in upper case with dashes '-' converted to underscore '_' and prefixed with PGCOMPARE_.  For example, batch-fetch-size can be set by using the environment variable PGCOMPARE_BATCH_FETCH_SIZE.

### System

#### batch-fetch-size

  Sets the fetch size for retrieving rows from the source or target database.

  Default:  2000

#### batch-commit-size

  The commit size controls the array size and number of rows concurrently inserted into the dc_source/dc_target staging tables.

  Default: 2000

#### batch-progress-report-size

  Defines the number of rows used in mod to report progress.

  Default: 1000000

#### column-hash-method

  Determines how the hash is performed.  Valid values are `database` and `hybrid`.  When set to `database` the column value hash is performed on the source/target database.  For `hybrid` the hash is performed by the pgCompare thread.

  Default:  database

#### database-sort

  Determines if the sorting of the rows based on primary key occurs on the source/target database.  If set to true, the default, the rows will be sorted before being compared.  If set to false, the sorting will take place in the repository database.

  Default: true

#### float-scale

  Set the preferred scale used to cast low precision numbers.

  Default: 3

#### loader-threads

  Sets the number of threads to load data into the temporary tables. Set to 0 to disable loader threads.

  Default: 0

#### log-level
  
  Level to determine the amount of log messages written to the log destination.

  Default: INFO

#### log-destination

  Location where log messages will be written.

  Default:  stdout

#### message-queue-size

  Size of message queue used by loader threads (nbr messages).
  
  Default: 100

#### number-cast

  Defines how numbers are cast for hash function (notation|standard).  Valid values are `notation` for scientific notation and `standard` for standard number casting.
  
  Default: notation

#### observer-throttle

  Set to true or false, instructs the loader threads to pause and wait for the observer thread to catch up before continuing to load more data into the staging tables.

  Default: true

#### observer-throttle-size

  Number of rows loaded before the loader thread will sleep and wait for clearance from the observer thread.

  Default: 2000000

#### observer-vacuum

  Set to true or false, instructs the observer whether to perform a vacuum on the staging tables during checkpoints.

  Default: true

#### stage-table-parallel

  Default parallel degree to set on staging table.

  Default: 0

#### standard-number-format
  
  Format used to cast numbers 
  
  Default: 0000000000000000000000.0000000000000000000000

#### batch-offset-size
  
  This configuration indicates from which data line the hash value comparison begins to be generated. 
  
  Default: 0
  
#### batch-compare-size
  
  This configuration indicates how many Hash values will be generated. 
  
  Default: 2000
  
These two configurations are used to paginate the data for querying when generating "hash comparison". For instance, only compare the data ranging from 1000 to 2000 or from 5000 to 10000.

#### batch-check-size
  
  This configuration indicates how many "check validations" are to be performed. 
  
  Default: 1000

### Repository

#### repo-dbname

  Repository database name.

#### repo-host

  Host name of server hosting the Postgres repository database.
 
#### repo-password

  Postgres database user password.
 
#### repo-port

  Repository Postgres instance port.

#### repo-schema

  Name of schema that owns the repository tables.

#### repo-sslmode

  Set the SSL mode to use for the database connection (disable|prefer|require)

#### repo-user

  Postgres database username.

### Source|Target

#### source|target-dbname

  Database or service name.

#### source|target-host

  Database server name.


#### source|target-password

  Database password.

#### source|target-port

  Database port.

#### source|target-schema

  Name of schema that owns the tables.

#### source|target-sslmode

  Set the SSL mode to use for the database connection (disable|prefer|require)

#### source|target-type

  Database type: oracle, postgres, mariadb, mssql, mysql, snowflake, db2

#### source|target-user

  Database username.

#### source|target-warehouse

  Used only for Snowflake, sets the virtual warehouse to be used for the compare operations.

## Property Precedence

The system contains default values for every parameter.  These can be over-ridden using environment variables, properties file, or values saved in the `dc_project` table.  The following is the order of precedence used:

- Default values
- Properties file
- Environment variables
- Settings stored in `dc_project` table

# License

**pgCompare** is licensed under the [Apache 2.0 license](LICENSE.md).
