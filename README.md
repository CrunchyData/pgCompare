<div>
  <h1 style="font-size: 70px;text-align: center">pgCompare</h1>
  <h2 style="text-align: center">Data Compare</h2>
</div>
<hr>

[![License](https://img.shields.io/github/license/CrunchyData/postgres-operator)](LICENSE.md)

# Data Compare Made Simple

**pgCompare** is a Java-based tool for validating data consistency after replication or migration between databases. It's designed for scenarios like:


- **Data migration from Oracle/DB2/MySQL/MSSQL to Postgres:**  Compare data during or post-migration.

- **Logical replication between same or different database platforms:** Validate data across platforms while minimizing database overhead.

- **Active-Active replication configuration:**  Regularly verify data consistency to mitigate risks.

pgCompare uses hashing to compare table data efficiently. Hash values for primary keys and remaining columns are stored in a repository, reducing storage and network demands. Comparisons are processed in parallel, improving performance.

This open-source project is maintained by **Crunchy Data** under the **Apache 2.0 License** and is made available for broader use, testing, and feedback.

# Features

- Supports Oracle, PostgreSQL, DB2, MySQL, and MSSQL.
- Efficient parallel comparisons using hashing.
- Handles batch processing for performance tuning.
- Stores configurations for multiple comparison projects in a central repository.

# Installation

## Requirements

Before initiating the build and installation process, ensure the following prerequisites are met:

1. **Java** 21 or later.
2. **Maven** 3.9 or later.
3. **Postgres** 15 or later (for the repository).
4. Supported JDBC drivers (DB2, Postgres, MySQL, MSSQL and Oracle currently supported).
5. Direct Postgres connections (e.g., no pgBouncer).

## Limitations

- Date/Timestamps compared only to the second (format: DDMMYYYYHH24MISS).
- Unsupported data types: blob, long, longraw, bytea.
- Cross-platform comparison limitations with boolean type.
- Reserved words cannot be used for table/column names.

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

At a minimal the `repo-xxxxx` parameters are required in the properties file (or specified by environment parameters).  Besides the properties file and environment variables, another alternative is to store the property settings in the `dc_project` table.  Settings can be stored in the `project_config` column in JSON format ({"parameter": "value"}).

## 4. Initialize Repository

Run the script or use the command below to set up the PostgreSQL repository:

```shell
java -jar pgcompare.jar --init
```

## 5. Discover Tables

Discover and map tables in specified schemas:

```shell
java -jar pgcompare.jar --discover
```

# Usage

## Define Table Mapping

1. Automatic Discovery

    Discover and map tables in specified schemas:

    ```shell
    java -jar pgcompare.jar --discover
    ```

2. Manual Registration 

    Insert mappings into `dc_table` and `dc_table_map` tables in the repository.

## Run Data Comparison

```shell
java -jar pgcompare.jar --batch 0
```

Batch 0 processes all data. Use `PGCOMPARE-BATCH` or specify the batch number using the `--batch` argument to specify a batch number.

## Recheck Discrepancies

Revalidate flagged rows:

```shell
java -jar pgcompare.jar --batch 0 --check
```

# Upgrading

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
SELECT compare_start, table_name, status, source_cnt AS total_cnt, equal_cnt, not_equal_cnt,
        missing_source_cnt + missing_target_cnt AS missing_cnt
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
      FULL OUTER JOIN dc_target t ON s.pk = t.pk;
```

# Reference

## Column Map

The system will automatically generate a column mapping during the first execution on a table.  This column mapping will be stored in the `dc_table_column` and `dc_table_column_map` repository tables. This mapping can be performed ahead of time or the generated mapping modified as needed.  If a column mapping is present, the program will not perform a remap unless instructed to using the `maponly` flag.

To create or overwrite current column mappings stored in column_map colum of dc_table, execute the following:

```shell
java -jar pgcompare.jar --batch 0 --maponly
```

## Properties

Properties are categorized into four sections: system, repository, source, and target. Each section has specific properties, as described in detail in the documentation.  The properties can be specified via a configuration file, environment variables or a combination of both.  To use environment variables, the environment variable will be the name of hte property in upper case prefixed with "PGCOMPARE-".  For example, batch-fetch-size can be set by using the environment variable PGCOMPARE-BATCH-FETCH-SIZE.

### System
- batch-fetch-size: Sets the fetch size for retrieving rows from the source or target database.
- batch-commit-size:  The commit size controls the array size and number of rows concurrently inserted into the dc_source/dc_target staging tables.
- batch-progress-report-size:  Defines the number of rows used in mod to report progress.
- database-source:  Determines if the sorting of the rows based on primary key occurs on the source/target database.  If set to true, the default, the rows will be sorted before being compared.  If set to false, the sorting will take place in the repository database.
- loader-threads: Sets the number of threads to load data into the temporary tables. Default is 4.  Set to 0 to disable loader threads.
- log-level:   Level to determine the amount of log messages written to the log destination.
- log-destination:  Location where log messages will be written.  Default is stdout.
- message-queue-size:  Size of message queue used by loader threads (nbr messages).  Default is 100.
- number-cast: Defines how numbers are cast for hash function (notation|standard).  Default is notation (for scientific notation).
- observer-throttle:  Set to true or false, instructs the loader threads to pause and wait for the observer thread to catch up before continuing to load more data into the staging tables.
- observer-throttle-size:  Number of rows loaded before the loader thread will sleep and wait for clearance from the observer thread.
- observer-vacuum:  Set to true or false, instructs the observer whether to perform a vacuum on the staging tables during checkpoints.

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
- source-password:  Database password.
- source-port:  Database port.
- source-schema:  Name of schema that owns the tables.
- source-sslmode: Set the SSL mode to use for the database connection (disable|prefer|require)
- source-type:  Database type: oracle, postgres
- source-user:   Database username.

### Target

- target-database-hash: True or false, instructs the application where the hash should be computed (on the database or by the application).
- target-dbname:  Database or service name.
- target-host:  Database server name.
- target-password:  Database password.
- target-port:  Database port.
- target-schema:  Name of schema that owns the tables.
- target-sslmode: Set the SSL mode to use for the database connection (disable|prefer|require)
- target-type:  Database type: oracle, postgres
- target-user:  Database username.

## Property Precedence

The system contains default values for every parameter.  These can be over-ridden using environment variables, properties file, or values saved in the `dc_project` table.  The following is the order of precedence used:

- Default values
- Properties file
- Environment variables
- Settings stored in `dc_project` table


# License

**pgCompare** is licensed under the [Apache 2.0 license](LICENSE.md).
