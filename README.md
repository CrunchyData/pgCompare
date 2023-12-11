<p align="center">
  <h1 align="center" style="font-size: 70px;">Confero</h1>
  <h2 align="center">Data Compare</h2>
</p>

[![License](https://img.shields.io/github/license/CrunchyData/postgres-operator)](LICENSE.md)

# Data Compare Made Simple

Confero is a Java application designed for use in situations where you are replicating data from different sources and need to validate that post-replication data consistency, including:

- **Data migration from Oracle to Postgres:**  Migrating from Oracle to Postgres? Confero can be utilized to compare data between Oracle and Postgres during and/or after the process of data migration.


- **Logical replication between same or different database platforms:** Confero exhibits enhanced optimization when comparing data, reducing the overhead on the source and target databases. The comparison tasks can also be delegated to a physical replication target. Confero not only identifies rows that appear out-of-sync but also offers the capability to revalidate those rows, proving valuable in the process of verifying data on active systems.


- **Active-Active replication configuration:**  There are inherent data consistency risks associated with any active-active database setup. To meet verification requirements, Confero can be employed regularly to compare either all or specific portions of the data.

At a higher level, Confero reads a row from a table and generates two hash values. The initial hash is executed on the primary key column(s), while the second hash is computed on the remaining columns. The hash can be calculated by the database or by the application. These hash values are stored in the Confero repository. Representing the original values as a hash minimizes the required space in the repository database and decreases network traffic. A parallel process consistently conducts comparisons on sets of data as they are loaded to expedite the comparison process and avoid row-by-row processing. Ultimately, a summary of the comparison results is displayed and stored in the Confero repository.

Confero is an open-source project maintained by the team at Crunchy Data and made available under the Apache 2.0 licenses for broader use, testing, and feedback.

Why the name Confero? The name is derived from the Latin word "cōnferō," meaning "to bring together."

# Installation

### Requirements
Before initiating the build and installation process, ensure the following prerequisites are met:

1. Java version 11 or higher.
2. Postgres version 15 or higher (to use for the Confero Data Compare repository).
3. Necessary JDBC drivers (Postgres and Oracle currently supported).

### Compile
Once the prerequisites are met, begin by forking the repository and cloning it to your host machine:

```sh
YOUR_GITHUB_UN="<your GitHub username>"
git clone --depth 1 "git@github.com:${YOUR_GITHUB_UN}/conferodc.git"
cd conferodc
```

Compile the Java source:

```sh
mvn clean install
```
### Configure Repository Database
Confero necessitates a hosted Postgres repository. To configure, connect to a Postgres database and execute the provided confero.sql script in the database directory.

# Getting Started

### Defining Table Mapping
The initial step involves defining a set of tables to compare, achieved by inserting rows into the `dc_table` within the Confero repository.


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

Example of loading a row into `dc_table`:


```sql
INSERT INTO dc_table (source_schema, source_table, target_schema, target_table, parallel_degree, status, batch_nbr)
  VALUES ('hr','emp','hr','emp',1,'ready',1);
```

### Create `confero.properties`
Copy the `confero.properties.sample` file to confero.properties and define the repository, source, and target connection parameters.  Refer to the Properties section for more details on the settings.

By default, the application looks for the properties file in the execution directory.  This can be overriden by using the CONFERODC_CONFIG environment variable to point to a file in a different location.

### Perform Data Compare
With the table mapping defined, execute the comparison and provide the mandatory batch command line argument:

```shell
java -jar conferodc --batch=0
```

Using a batch value of 0 will execute the action for all batches.

### Debug/Recheck Out-of-Sync Rows
If discrepancies are detected, run the comparison with the 'check' option:

```shell
java -jar conferodc --batch=0 --check
```

This recheck process is useful when transactions may be in flight during the initial comparison.  The recheck only checks the rows that have been flagged with a descrepancy.  If the rows still do not match, details will be reported.  Otherwise, the rows will be cleared and marked in-sync.


# Properties
Properties are categorized into four sections: system, repository, source, and target. Each section has specific properties, as described in detail in the documentation.  The properties can be specified via a configuration file, environment variables or a combination of both.  To use environment variables, the environment variable will be the name of hte property in upper case prefixed with "CONFERODC-".  For example, batch-fetch-size can be set by using the environment variable CONFERODC-BATCH-FETCH-SIZE.

### system
- batch-fetch-size: Sets the fetch size for retrieving rows from the source or target database.
- batch-commit-size:  The commit size controls the array size and number of rows concurrently inserted into the dc_source/dc_target staging tables.
- batch-load-size:  Defines the number of loads retrieved before saving to the staging tables.
- observer-throttle:  Set to true or false, instructs the loader threads to pause and wait for the observer thread to catch up before continuing to load more data into the staging tables.
- observer-throttle-size:  Number of rows loaded before the loader thread will sleep and wait for clearance from the observer thread.
- observer-vacuum:  Set to true or false, instructs the observer whether to perform a vacuum on the staging tables during checkpoints.

### repository
- repo-host: Host name of server hosting the Postgres repository database.
- repo-port:  Repository Postgres instance port.
- repo-dbname:  Repository database name.
- repo-user:  Postgres database username.
- repo-password:  Postgres database user password.
- repo-schema:  Name of schema that owns the repository tables.

### source
- source-name:  User defined name for the source.
- source-type:  Database type: oracle, postgres
- source-host:  Database server name.
- source-port:  Database port.
- source-dbname:  Database or service name.
- source-user:   Database username.
- source-password:  Database password.
- source-database-hash: True or false, instructs the application where the hash should be computed (on the database or by the application).

### target
- target-name:  User defined name for the target.
- target-type:  Database type: oracle, postgres
- target-host:  Database server name.
- target-port:  Database port.
- target-dbname:  Database or service name.
- target-user:  Database username.
- target-password:  Database password.
- target-database-hash: True or false, instructs the application where the hash should be computed (on the database or by the application).

# Data Compare
Confero stores a hash representation of primary key columns and other table columns, reducing row size and storage demands. The utility optimizes network traffic and speeds up the process by using hash functions when comparing similar platforms.

## Hash Options
By default, the data is normalized and the hash performed by the Java code.  For supported platforms (Oracle 12.1 or higher and Postgres 11 or higher), the progrom will leverage database functions to perform the hash.  Allowing the database to perform the hash does increase CPU load on the hosting server, but reduces network traffic, increases speed, and reduces memory requirements of Confero.

## Processes
Each comparison involves at least three threads: one for the observer and two for the source and target loader processes. By specifying a mod_column in the dc_tables and increasing parallel_degree, the number of threads can be increased to speed up comparison. Tuning between batch sizes, commit rates, and parallel degree is essential for optimal performance.

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

Confero project source code is available subject to the [Apache 2.0 license](LICENSE.md).
