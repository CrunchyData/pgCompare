# pgCompare Java Application Architecture

## Overview
pgCompare is a database comparison and reconciliation tool that compares tables between source and target databases, and identifies differences.

---

## Application Flow

```
┌──────────────────────────────────────────────────────────────────────────┐
│                              MAIN ENTRY POINT                             │
│                           pgCompare.main()                                │
└────────────────────┬─────────────────────────────────────────────────────┘
                     │
                     ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                      COMMAND LINE PARSING                                │
│                    CommandLineParser.parse()                             │
│  Options: --batch, --project, --report, --table, --help, --version       │
└────────────────────┬─────────────────────────────────────────────────────┘
                     │
                     ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                   APPLICATION INITIALIZATION                             │
│               ApplicationContext.initialize()                            │
│  - Load Settings                                                         │
│  - Connect to Repository DB                                              │
│  - Load Project Config                                                   │
│  - Run Validation                                                        │
└────────────────────┬─────────────────────────────────────────────────────┘
                     │
                     ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                    ACTION EXECUTION                                      │
│              ApplicationContext.executeAction()                          │
│                                                                          │
│  ┌──────────────┬──────────────┬──────────────┬──────────────┐           │
│  │              │              │              │              │           │
│  ▼              ▼              ▼              ▼              ▼           │
│  INIT        DISCOVER       COMPARE        CHECK       COPY-TABLE        │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Package Structure

### 1. **Root Package** (`com.crunchydata`)
- **pgCompare** - Main entry point

### 2. **Config Package** (`com.crunchydata.config`)
Configuration and initialization classes

### 3. **Controller Package** (`com.crunchydata.controller`)
High-level orchestration and business logic

### 4. **Core Package** (`com.crunchydata.core`)
Core processing logic (threading, comparison, database operations)

### 5. **Model Package** (`com.crunchydata.model`)
Data transfer objects and domain models

### 6. **Service Package** (`com.crunchydata.service`)
Reusable services for database operations, SQL generation, etc.

### 7. **Util Package** (`com.crunchydata.util`)
Utility classes for common operations

---

## Class Hierarchy and Methods

### **Root Package**

#### `pgCompare`
**Purpose:** Application entry point
```
public class pgCompare
  + main(String[] args)
```

**Calls:**
- CommandLineParser.parse()
- ApplicationContext (constructor)
- ApplicationContext.initialize()
- ApplicationContext.executeAction()
- LoggingUtils.write()

---

### **Config Package**

#### `ApplicationContext`
**Purpose:** Manages application lifecycle and context
```
public class ApplicationContext
  - cmd: CommandLine
  - pid: Integer
  - action: String
  - batchParameter: Integer
  - connRepo: Connection
  - connSource: Connection
  - connTarget: Connection
  
  + ApplicationContext(CommandLine cmd)
  + initialize()
  + executeAction()
  + getPid()
  + getBatchParameter()
  + getConnRepo()
  + getConnSource()
  + getConnTarget()
  - connectToRepository()
  - connectToSourceAndTarget()
  - setProjectConfig(Connection conn, Integer pid)
  - logStartupInfo()
  - logConfigurationParameters()
  - handleRepoInitialization()
  - performDiscovery()
  - performCompare()
  - performCopyTable()
```

**Calls:**
- Settings.Props
- Settings.setProjectConfig()
- ValidationUtils.all()
- LoggingUtils
- DatabaseConnectionService.getConnection()
- RepositoryInitializationService.createRepository()
- DiscoverController.discoverTables()
- DiscoverController.discoverColumns()
- CompareController.performCompare()
- TableController.performCopyTable()

#### `CommandLineParser`
**Purpose:** Parse command line arguments
```
public class CommandLineParser
  + parse(String[] args): CommandLine
  + showHelp()
  + showVersion()
  - createOptions(): Options
```

**Calls:**
- Settings.VERSION

#### `Settings`
**Purpose:** Application settings and configuration
```
public class Settings
  + VERSION: String
  + Props: Properties
  
  + setEnvironment(Properties prop): Properties
  + setProjectConfig(Connection conn, Integer pid)
```

**Calls:**
- FileSystemUtils.loadPropertiesFromFile()
- SQLExecutionHelper.simpleSelectReturnString()
- JsonProcessingUtils.mergeJsonObjects()

#### `config.sql.*` (8 interface files)
**Purpose:** SQL constant definitions per database platform
```
public interface DB2SQLConstants
public interface MariaDBSQLConstants
public interface MSSQLSQLConstants
public interface MYSQLSQLConstants
public interface OracleSQLConstants
public interface PostgresSQLConstants
public interface RepoSQLConstants
public interface SnowflakeSQLConstants
```

---

### **Controller Package**

#### `CompareController`
**Purpose:** Orchestrates comparison and recheck operations
```
public class CompareController
  + performCompare(ApplicationContext context)
  + reconcileData(Connection connRepo, Connection connSource, Connection connTarget,
                 Integer rid, Boolean check, DataComparisonTable dct,
                 DataComparisonTableMap dctmSource, DataComparisonTableMap dctmTarget,
                 ColumnMetadata ciSource, ColumnMetadata ciTarget, Integer cid): JSONObject
  - validatePerformCompareInputs(ApplicationContext context)
```

**Calls:**
- TableController.reconcileTables()
- RepoController.getTables()
- ReportController.createSummary()
- ThreadManager.createComparison()
- DataValidationThread.checkRows()
- ResultProcessor.processResults()

#### `TableController`
**Purpose:** Manages table operations and processing
```
public class TableController
  + getTableMap(Connection conn, Integer tid, String tableOrigin): DataComparisonTableMap
  + performCopyTable(ApplicationContext context): int
  + reconcileTables(CachedRowSet tablesResultSet, boolean isCheck, 
                   RepoController repoController, ApplicationContext context): ComparisonResults
  + createDCTableFromResultSet(CachedRowSet resultSet, Integer pid): DataComparisonTable
  - reconcileEnabledTable(DataComparisonTable table, boolean isCheck,
                         RepoController rpc, ApplicationContext context): JSONObject
  - createSkippedTableResult(DataComparisonTable table): JSONObject
  
  record ComparisonResults(int tablesProcessed, JSONArray runResults)
```

**Calls:**
- SQLExecutionHelper.simpleSelect()
- CompareController.reconcileData()
- ColumnManagementService (various methods)
- ColumnMetadataBuilder.build()

#### `ColumnController`
**Purpose:** Manages column metadata operations
```
public class ColumnController
  + getColumnInfo(JSONObject columnMap, String targetType, String platform, 
                 String schema, String table, Boolean useDatabaseHash): ColumnMetadata
```

**Calls:**
- DatabaseMetadataService (platform methods)
- ColumnMetadataBuilder.build()

#### `DiscoverController`
**Purpose:** Orchestrates discovery of tables and columns
```
public class DiscoverController
  + discoverColumns(Properties props, Integer pid, String table,
                   Connection connRepo, Connection connSource, Connection connTarget)
  + discoverTables(Properties Props, Integer pid, String table, 
                  Connection connRepo, Connection connSource, Connection connTarget)
  - cleanupPreviousDiscovery(Connection connRepo, String sql, ArrayList<Object> binds)
  - discoverTables(Properties props, Integer pid, String table,
                  Connection connRepo, Connection conn, String origin, boolean populateDCTable)
```

**Calls:**
- ColumnDiscoveryService.discoverColumns()
- DatabaseMetadataService.getTables()
- SQLExecutionHelper (various methods)

#### `RepoController`
**Purpose:** Repository database operations
```
public class RepoController
  + completeTableHistory(Connection conn, Integer tid, Integer batchNbr,
                        Integer rowCount, String actionResult)
  + createCompareId(Connection connRepo, DataComparisonTableMap dctmTarget, long rid): Integer
  + deleteDataCompare(Connection conn, Integer tid, Integer batchNbr)
  + dcrCreate(Connection conn, int tid, String tableName, long rid): Integer
  + dcrUpdateRowCount(Connection conn, String targetType, Integer cid, Integer rowCount)
  + getTables(Integer pid, Connection conn, Integer batchParameter, String table, Boolean check): CachedRowSet
  + saveTableColumn(Connection conn, DataComparisonTableColumn dctc): DataComparisonTableColumn
  + saveTableColumnMap(Connection conn, DataComparisonTableColumnMap dctcm)
  + startTableHistory(Connection conn, Integer tid, Integer batchNbr): Integer
  + vacuumRepo(Connection conn)
```

**Calls:**
- SQLExecutionHelper (various methods)
- TableManagementService (various methods)

#### `ReportController`
**Purpose:** Generate comparison reports and summaries
```
public class ReportController
  
  record SummaryStatistics(int totalRows, int outOfSyncRows, long elapsedTime) {
    + getThroughput(): long
  }
  
  + generateHtmlReport(JSONArray report, String filePath, String title)
  + generateCompleteReport(ApplicationContext context, int tablesProcessed,
                          JSONArray runResult, SummaryStatistics stats, boolean isCheck)
  + createSummary(ApplicationContext context, int tablesProcessed,
                 JSONArray runResult, boolean isCheck)
  - createJobSummaryData(int tablesProcessed, SummaryStatistics stats): JSONObject
  - createJobSummaryLayout(): JSONArray
  - createRunResultLayout(): JSONArray
  - createCheckResultLayout(): JSONArray
  - createSection(String title, JSONArray data, JSONArray layout): JSONObject
  - addCheckResultsToReport(JSONArray reportArray, JSONArray runResult)
  - calculateSummaryStatistics(JSONArray runResult): SummaryStatistics
```

**Calls:**
- HTMLWriterUtils (various methods)
- DisplayOperations (various methods)

---

### **Core Package**

#### `core.comparison.ResultProcessor`
**Purpose:** Process and finalize comparison results
```
public class ResultProcessor
  + processResults(Connection connRepo, long tid, int cid): JSONObject
  - configureDatabase(Connection connRepo)
  - calculateReconciliationStats(Connection connRepo, long tid): ReconciliationStats
  - updateResultWithStats(JSONObject result, ReconciliationStats stats)
  - updateDatabaseResults(Connection connRepo, JSONObject result, int cid)
```

**Calls:**
- SQLExecutionHelper (various methods)

#### `core.database.SQLExecutionHelper`
**Purpose:** Database-agnostic SQL execution utilities
```
public class SQLExecutionHelper
  
  record QueryResult<T>(T result, boolean success, String errorMessage, long executionTimeMs)
  
  + simpleSelect(Connection conn, String sql, ArrayList<Object> binds): CachedRowSet
  + simpleSelectWithResult(Connection conn, String sql, ArrayList<Object> binds): QueryResult<CachedRowSet>
  + simpleSelectReturnInteger(Connection conn, String sql, ArrayList<Object> binds): Integer
  + simpleSelectReturnString(Connection conn, String sql, ArrayList<Object> binds): String
  + simpleUpdate(Connection conn, String sql, ArrayList<Object> binds, Boolean commit): Integer
  + executeBatch(Connection conn, List<String> sqlStatements, boolean commit): int[]
  + simpleUpdateReturning(Connection conn, String sql, ArrayList<Object> binds): CachedRowSet
  + simpleUpdateReturningInteger(Connection conn, String sql, ArrayList<Object> binds): Integer
  + simpleExecute(Connection conn, String sql)
  - bindParameters(PreparedStatement stmt, ArrayList<Object> binds)
  - validateParameters(Connection conn, String sql, ArrayList<Object> binds)
```

**Calls:**
- LoggingUtils.write()

#### `core.database.ColumnMetadataBuilder`
**Purpose:** Build column metadata for comparisons
```
public class ColumnMetadataBuilder
  + build(JSONObject columnMap): ColumnMetadata
  - buildColumnMetadata(CachedRowSet columns, String platform): ColumnMetadata
  - buildColumnExpressionList(JSONArray columns): String
  - buildPKExpressionList(JSONArray columns): String
  - buildPKJSON(JSONArray columns): String
```

**Calls:**
- DatabaseMetadataService (enum methods)
- SQLExecutionHelper.simpleSelect()

#### `core.database.SnowflakeHelper`
**Purpose:** Snowflake-specific helper operations
```
public class SnowflakeHelper
  + getMetadataColumns(Connection conn, String schema, String table): JSONArray
  + buildSnowflakeSQL(String sql, Integer tid): String
```

**Calls:**
- SQLExecutionHelper.simpleSelect()

#### `core.threading.DataComparisonThread`
**Purpose:** Thread for loading data from source/target
```
public class DataComparisonThread extends Thread
  - tid, batchNbr, cid: Integer
  - parallelDegree, threadNumber: Integer
  - modColumn, pkList, stagingTable, targetType: String
  - sql: String
  - q: BlockingQueue<DataComparisonResult[]>
  - ts: ThreadSync
  - useDatabaseHash: Boolean
  
  + DataComparisonThread(Integer threadNumber, DataComparisonTable dct, 
                         DataComparisonTableMap dctm, ColumnMetadata cm, Integer cid, 
                         ThreadSync ts, Boolean useDatabaseHash, String stagingTable, 
                         BlockingQueue<DataComparisonResult[]> q)
  + run()
  - initializeRepositoryConnection(String threadName): Connection
  - initializeSourceTargetConnection(String threadName): Connection
  - handleLoaderThreadBatch(String threadName, DataComparisonResult[] dc, int batchCommitSize)
  - handleDirectDatabaseBatch(PreparedStatement stmtLoad, Connection connRepo)
  - handleObserverCoordination(String threadName, boolean firstPass, boolean observerThrottle, 
                                RepoController rpc, Connection connRepo, int rowsToReport)
  - processRemainingRecords(boolean useLoaderThreads, DataComparisonResult[] dc, 
                            PreparedStatement stmtLoad, RepoController rpc, 
                            Connection connRepo, int rowsToReport)
  - waitForQueuesToEmpty(String threadName)
  - signalThreadCompletion()
  - cleanupResources(String threadName, ResultSet rs, PreparedStatement stmt, 
                     PreparedStatement stmtLoad, Connection connRepo, Connection conn)
```

**Calls:**
- DatabaseConnectionService.getConnection()
- HashingUtils.getMd5()
- RepoController.dcrUpdateRowCount()
- LoggingUtils.write()
- ThreadSync.observerWait()

#### `core.threading.DataLoaderThread`
**Purpose:** Thread for loading data into staging tables
```
public class DataLoaderThread extends Thread
  - q: BlockingQueue<DataComparisonResult[]>
  - stagingTable: String
  - connRepo: Connection
  
  + DataLoaderThread(BlockingQueue<DataComparisonResult[]> q, String stagingTable, 
                     Connection connRepo)
  + run()
  - processQueue()
  - loadBatch(DataComparisonResult[] results)
  - cleanup()
```

**Calls:**
- SQLExecutionService.simpleUpdate()
- LoggingUtils.write()

#### `core.threading.DataValidationThread`
**Purpose:** Validate and check out-of-sync rows
```
public class DataValidationThread
  + checkRows(Connection repoConn, Connection sourceConn, Connection targetConn,
              DataComparisonTable dct, DataComparisonTableMap dctmSource, 
              DataComparisonTableMap dctmTarget, ColumnMetadata ciSource, 
              ColumnMetadata ciTarget, Integer cid): JSONObject
  + reCheck(Connection repoConn, Connection sourceConn, Connection targetConn,
            DataComparisonTableMap dctmSource, DataComparisonTableMap dctmTarget, 
            String pkList, ArrayList<Object> binds, DataComparisonResult dcRow, 
            Integer cid): JSONObject
  - extractColumnValue(CachedRowSet rowSet, int columnIndex): String
  - removeInSyncRow(Connection repoConn, DataComparisonResult dcRow)
  - updateResultCounts(Connection repoConn, JSONObject rowResult, 
                       CachedRowSet sourceRow, CachedRowSet targetRow, Integer cid)
```

**Calls:**
- SQLExecutionService.simpleSelect()
- SQLExecutionService.simpleUpdate()
- ColumnMetadataUtils.createColumnFilterClause()
- ColumnMetadataUtils.findColumnAlias()
- DatabaseMetadataService.getQuoteChar()
- DataProcessingUtils.convertClobToString()
- SQLFixGenerationService.generateFixSQL()

#### `core.threading.ObserverThread`
**Purpose:** Monitor and coordinate comparison threads
```
public class ObserverThread extends Thread
  - tid: Integer
  - cid: Integer
  - stagingTableSource, stagingTableTarget: String
  - ts: ThreadSync
  - loaderThreads: int
  
  + ObserverThread(Integer tid, Integer cid, String stagingTableSource, 
                   String stagingTableTarget, ThreadSync ts, int loaderThreads)
  + run()
  - executeReconciliationObserver(String threadName, Connection repoConn, 
                                  ArrayList<Object> binds, int cntEqual, int deltaCount, 
                                  int loaderThreads, DecimalFormat formatter, 
                                  int lastRun, RepoController rpc, int sleepTime)
  - handleNoMatches(int cntEqual, int deltaCount, ArrayList<Object> binds, 
                    RepoController rpc, Connection repoConn, PreparedStatement stmtSUS)
  - isReconciliationComplete(int tmpRowCount, int loaderThreads): boolean
  - handleSleepTiming(int tmpRowCount, int cntEqual, int sleepTime)
  - performCleanup(String threadName, Connection repoConn, RepoController rpc)
```

**Calls:**
- DatabaseConnectionService.getConnection()
- RepoController.dcrUpdateRowCount()
- RepoController.vacuumRepo()
- ThreadSync methods

#### `core.threading.ThreadManager`
**Purpose:** Manage thread lifecycle
```
public class ThreadManager
  + createComparison(Connection connRepo, Integer cid, DataComparisonTable dct,
                    DataComparisonTableMap dctmSource, DataComparisonTableMap dctmTarget,
                    ColumnMetadata ciSource, ColumnMetadata ciTarget)
  - createLoaderThreads(BlockingQueue<DataComparisonResult[]> queueSource,
                       BlockingQueue<DataComparisonResult[]> queueTarget,
                       String stagingTableSource, String stagingTableTarget,
                       Connection connRepo): List<DataLoaderThread>
  - createComparisonThreads(DataComparisonTable dct, DataComparisonTableMap dctmSource,
                           DataComparisonTableMap dctmTarget, ColumnMetadata ciSource,
                           ColumnMetadata ciTarget, Integer cid, ThreadSync ts,
                           BlockingQueue queueSource, BlockingQueue queueTarget,
                           String stagingTableSource, String stagingTableTarget): List<Thread>
  - startAllThreads(List<Thread> threads)
  - waitForAllThreads(List<Thread> threads)
```

**Calls:**
- DataComparisonThread (constructor)
- DataLoaderThread (constructor)
- ObserverThread (constructor)
- StagingTableService.createStagingTable()
- StagingTableService.dropStagingTable()

#### `core.threading.ThreadSync`
**Purpose:** Thread synchronization object
```
public class ThreadSync
  + sourceComplete: boolean
  + targetComplete: boolean
  + sourceWaiting: boolean
  + targetWaiting: boolean
  + stopObserver: boolean
  
  + observerWait()
  + observerNotify()
```

---

### **Model Package**

#### `ColumnMetadata`
**Purpose:** Column metadata model
```
@Data
@Builder
public class ColumnMetadata
  + columnList: String
  + nbrColumns: Integer
  + nbrPKColumns: Integer
  + columnExpressionList: String
  + pkExpressionList: String
  + pkList: String
  + pkJSON: String
```

#### `DataComparisonResult`
**Purpose:** Comparison result model
```
@Data
@AllArgsConstructor
@NoArgsConstructor
public class DataComparisonResult
  + tid: Integer
  + tableName: String
  + pkHash: String
  + columnHash: String
  + pk: String
  + compareResult: String
  + threadNbr: Integer
  + batchNbr: Integer
```

#### `DataComparisonTable`
**Purpose:** Table configuration model
```
@Data
@AllArgsConstructor
@NoArgsConstructor
public class DataComparisonTable
  + pid: Integer
  + tid: Integer
  + tableAlias: String
  + enabled: boolean
  + batchNbr: Integer
  + parallelDegree: Integer
```

#### `DataComparisonTableColumn`
**Purpose:** Column configuration model
```
@Data
@AllArgsConstructor
@NoArgsConstructor
public class DataComparisonTableColumn
  + tid: Integer
  + columnId: Integer
  + columnAlias: String
  + enabled: boolean
```

#### `DataComparisonTableColumnMap`
**Purpose:** Column mapping model
```
@Data
@AllArgsConstructor
@NoArgsConstructor
public class DataComparisonTableColumnMap
  + columnId: Integer
  + columnOrigin: String
  + columnName: String
  + dataType: String
  + dataClass: String
  + dataLength: Integer
  + numberPrecision: Integer
  + numberScale: Integer
  + columnNullable: boolean
  + columnPrimarykey: boolean
  + mapExpression: String
  + supported: boolean
  + preserveCase: boolean
  + mapType: String
```

#### `DataComparisonTableMap`
**Purpose:** Table mapping model
```
@Data
@AllArgsConstructor
@NoArgsConstructor
public class DataComparisonTableMap
  + tid: Integer
  + destType: String
  + schemaName: String
  + tableName: String
  + modColumn: String
  + tableFilter: String
  + tablePreserveCase: boolean
  + schemaPreserveCase: boolean
  + batchNbr: Integer
  + compareSQL: String
  + tableAlias: String
  + pid: Integer
```

---

### **Service Package**

#### `DatabaseConnectionService`
**Purpose:** Manage database connections across different platforms
```
public class DatabaseConnectionService
  + isConnectionValid(Connection conn): boolean
  + getConnection(String platform, String destType): Connection
  - buildConnectionUrl(DatabasePlatform platform, String destType): String
  - buildConnectionProperties(DatabasePlatform platform, String destType): Properties
  - configureConnection(Connection conn, DatabasePlatform platform)
```

**Calls:**
- DatabaseMetadataService.DatabasePlatform
- Settings.Props
- LoggingUtils.write()

#### `DatabaseMetadataService`
**Purpose:** Database platform configuration and metadata operations
```
public class DatabaseMetadataService
  
  enum DatabasePlatform {
    DB2, ORACLE, MARIADB, MYSQL, MSSQL, POSTGRES, SNOWFLAKE
    
    + getName(): String
    + getUrlTemplate(): String
    + isAutoCommit(): boolean
    + requiresAnsiMode(): boolean
    + getNativeCase(): String
    + getQuoteChar(): String
    + getColumnHashTemplate(): String
    + getConcatOperator(): String
    + getReplacePKSyntax(): String
    + fromString(String platform): DatabasePlatform
  }
  
  + getNativeCase(String platform): String
  + getQuoteChar(String platform): String
  + getConcatOperator(String platform): String
  + getReplacePKSyntax(String platform): String
  + getTables(Connection conn, String schema, String tableFilter, String sql): JSONArray
```

**Calls:**
- DataProcessingUtils.ShouldQuoteString()
- LoggingUtils.write()

#### `SQLSyntaxService`
**Purpose:** SQL generation and syntax utilities
```
public class SQLSyntaxService
  + buildGetTablesSQL(Integer batchNbr, String table, Boolean check): String
  + generateCompareSQL(DataComparisonTableMap dctmSource, DataComparisonTableMap dctmTarget,
                      ColumnMetadata ciSource, ColumnMetadata ciTarget)
  + buildLoadSQL(String columnHashMethod, DataComparisonTableMap tableMap, 
                ColumnMetadata columnMetadata): String
```

**Calls:**
- DatabaseMetadataService.DatabasePlatform
- DataProcessingUtils.ShouldQuoteString()
- Settings.Props
- LoggingUtils.write()

#### `SQLFixGenerationService`
**Purpose:** Generate SQL statements to fix data differences
```
public class SQLFixGenerationService
  + generateFixSQL(Connection sourceConn, Connection targetConn,
                  DataComparisonTableMap dctmSource, DataComparisonTableMap dctmTarget,
                  ArrayList<Object> binds, DataComparisonResult dcRow,
                  JSONObject rowResult): String
  + generateFixSQLForMultipleRows(Connection sourceConn, Connection targetConn,
                                  DataComparisonTableMap dctmSource,
                                  DataComparisonTableMap dctmTarget,
                                  JSONArray checkResults): List<String>
  - isMissingSource(JSONObject rowResult): boolean
  - isMissingTarget(JSONObject rowResult): boolean
  - isNotEqual(JSONObject rowResult): boolean
  - generateDeleteSQL(DataComparisonTableMap dctmTarget, JSONObject pk): String
  - generateInsertSQL(Connection sourceConn, DataComparisonTableMap dctmSource,
                     DataComparisonTableMap dctmTarget, ArrayList<Object> binds,
                     JSONObject pk): String
  - generateUpdateSQL(Connection sourceConn, Connection targetConn,
                     DataComparisonTableMap dctmSource, DataComparisonTableMap dctmTarget,
                     ArrayList<Object> binds, JSONObject pk, JSONObject rowResult): String
  - buildWhereClause(JSONObject pk, String quoteChar): String
  - formatValue(Object value): String
  - buildFilterFromPK(Connection conn, JSONObject pk, String quoteChar): String
```

**Calls:**
- SQLExecutionHelper.simpleSelect()
- DatabaseMetadataService.getQuoteChar()
- DataProcessingUtils.ShouldQuoteString()
- LoggingUtils.write()

#### `ColumnDiscoveryService`
**Purpose:** Discover and process columns from databases
```
public class ColumnDiscoveryService
  + discoverColumns(Properties props, Integer pid, String table, 
                   Connection connRepo, Connection connSource, Connection connTarget)
  + loadColumns(Properties props, Integer tid, String schema, String tableName,
               Connection connRepo, Connection connDest, String destRole, 
               Boolean populateDCTableColumn)
  - clearPreviousMappings(Connection connRepo, Integer pid, String table)
  - discoverColumnsForRole(Properties props, Integer pid, String table,
                          Connection connRepo, Connection connDest, String role)
  - findExistingColumn(Connection connRepo, Integer tid, String columnName): Integer
  - createOrUpdateColumn(Integer tid, String columnName, Integer cid,
                        Boolean populateDCTableColumn, Connection connRepo): DataComparisonTableColumn
  - createColumnMapping(Connection connRepo, Integer tid, Integer columnID,
                       String destRole, JSONObject columnInfo)
```

**Calls:**
- SQLExecutionHelper (various methods)
- ColumnMetadataUtils.getColumns()
- RepoController.saveTableColumn()
- RepoController.saveTableColumnMap()

#### `ColumnManagementService`
**Purpose:** Manage column metadata and mappings
```
public class ColumnManagementService
  + getColumnMapping(Connection connRepo, Integer tid): String
  + getSourceColumnMetadata(JSONObject columnMap, DataComparisonTableMap dctmSource): ColumnMetadata
  + getTargetColumnMetadata(JSONObject columnMap, DataComparisonTableMap dctmTarget, Boolean check): ColumnMetadata
  + saveTableColumn(Connection conn, DataComparisonTableColumn dctc): DataComparisonTableColumn
  - validateTableColumnInputs(Connection conn, DataComparisonTableColumn dctc)
```

**Calls:**
- SQLExecutionHelper (various methods)
- ColumnController.getColumnInfo()

#### `TableManagementService`
**Purpose:** Manage table configurations and operations
```
public class TableManagementService
  + saveTable(Connection conn, DataComparisonTable dataComparisonTable): DataComparisonTable
  + saveTableMap(Connection conn, DataComparisonTableMap dataComparisonTableMap)
  + getTables(Integer pid, Connection conn, Integer batchNbr, String table, Boolean check): CachedRowSet
  + completeTableHistory(Connection conn, Integer tid, Integer batchNbr, 
                        Integer rowCount, String actionResult)
  + deleteDataCompare(Connection conn, Integer tid, Integer batchNbr)
  - buildGetTablesSQL(Integer batchNbr, String table, Boolean check): String
  - buildGetTablesBinds(Integer pid, Integer batchNbr, String table): ArrayList<Object>
```

**Calls:**
- SQLExecutionHelper (various methods)
- LoggingUtils.write()

#### `StagingTableService`
**Purpose:** Manage staging tables for data loading
```
public class StagingTableService
  + createStagingTable(Connection conn, String location, Integer tid, Integer threadNbr): String
  + dropStagingTable(Connection conn, String stagingTable)
  + loadFindings(Connection conn, String location, Integer tid, String stagingTable, 
                Integer batchNbr, Integer threadNbr, String tableAlias)
  - validateCreateStagingTableInputs(Connection conn, String location, Integer tid, Integer threadNbr)
  - validateDropStagingTableInputs(Connection conn, String stagingTable)
  - validateLoadFindingsInputs(Connection conn, String location, Integer tid, String stagingTable, 
                               Integer batchNbr, Integer threadNbr, String tableAlias)
```

**Calls:**
- SQLExecutionHelper.simpleExecute()
- SQLExecutionHelper.simpleUpdate()
- Settings.Props
- LoggingUtils.write()

#### `RepositoryInitializationService`
**Purpose:** Initialize and setup repository database
```
public class RepositoryInitializationService
  
  enum DDLPhase {
    SCHEMA_CREATION, TABLE_CREATION, INDEX_CREATION, 
    DATA_INSERTION, FUNCTION_CREATION
  }
  
  + createRepository(Properties props, Connection conn)
  - createSchema(Properties props, Connection conn)
  - createTables(Connection conn)
  - createIndexesAndConstraints(Connection conn)
  - insertInitialData(Connection conn)
  - createFunctions(Connection conn)
  - executeDDL(Connection conn, String ddl, DDLPhase phase)
  - getPropertyWithDefault(Properties props, String key, String defaultValue): String
```

**Calls:**
- DatabaseConnectionService.isConnectionValid()
- SQLExecutionHelper.simpleUpdate()
- RepoSQLConstants (SQL definitions)
- LoggingUtils.write()

---

### **Util Package**

#### `LoggingUtils`
**Purpose:** Logging operations and message formatting
```
public class LoggingUtils
  + initialize()
  + write(String level, String threadName, String message)
  + writeException(String level, String threadName, Exception e)
  - getLogger(): Logger
  - formatMessage(String threadName, String message): String
```

#### `ValidationUtils`
**Purpose:** Validation operations for connections and configuration
```
public class ValidationUtils
  + all(String action): boolean
  + validateConnection(Connection conn): boolean
  + validateTable(String tableName): boolean
  + validateSchema(String schemaName): boolean
```

**Calls:**
- LoggingUtils.write()

#### `HashingUtils`
**Purpose:** Hashing operations for data comparison
```
public class HashingUtils
  + getMd5(String input): String
  + getSha256(String input): String
```

#### `ColumnMetadataUtils`
**Purpose:** Column metadata utilities and operations
```
public class ColumnMetadataUtils
  + getColumns(Properties Props, Connection conn, String schema, String table, 
               String destRole): JSONArray
  + createColumnFilterClause(Connection conn, Integer tid, String columnAlias,
                            String origin, String quoteChar): String
  + findColumnAlias(JSONArray columns, String columnName, String origin): String
  - buildColumnQuery(String platform, String schema, String table): String
```

**Calls:**
- SQLExecutionHelper.simpleSelect()
- Platform-specific SQL constants

#### `DataProcessingUtils`
**Purpose:** Data processing and string utilities
```
public class DataProcessingUtils
  + ShouldQuoteString(boolean preserveCase, String value, String quoteChar): String
  + preserveCase(String value, boolean preserveCase, String nativeCase): String
  + convertClobToString(SerialClob clob): String
  + trimString(String value, int maxLength): String
```

#### `DataTypeCastingUtils`
**Purpose:** Data type conversion and casting operations
```
public class DataTypeCastingUtils
  + castToInteger(Object value): Integer
  + castToLong(Object value): Long
  + castToBoolean(Object value): Boolean
  + castToString(Object value): String
```

#### `JsonProcessingUtils`
**Purpose:** JSON processing and manipulation utilities
```
public class JsonProcessingUtils
  + findOne(JSONArray jsonArray, String key, String value): JSONObject
  + mergeJsonObjects(JSONObject base, JSONObject overlay): JSONObject
  + arrayToList(JSONArray array): List<Object>
```

#### `FileSystemUtils`
**Purpose:** File system operations and I/O
```
public class FileSystemUtils
  + loadPropertiesFromFile(String fileName): Properties
  + writeFile(String fileName, String content)
  + readFile(String fileName): String
  + fileExists(String fileName): boolean
```

#### `DisplayOperations`
**Purpose:** Console display formatting and output utilities
```
public class DisplayOperations
  + displayTableSummaries(JSONArray runResult)
  + displayJobSummary(int tablesProcessed, SummaryStatistics stats)
  + displayNoTablesMessage(boolean isCheck)
  + displayIndividualTableSummary(JSONObject tableResult, int indent)
  + printSummary(String message, int indent)
```

**Calls:**
- LoggingUtils.write()

#### `HTMLWriterUtils`
**Purpose:** HTML report generation utilities
```
public class HTMLWriterUtils
  + writeHtmlContent(FileWriter writer, JSONArray report)
  + writeHtmlHeader(FileWriter writer, String title)
  + writeTableHeader(FileWriter writer, JSONArray layout)
  + writeTableRows(FileWriter writer, JSONArray sectionData, JSONArray layout)
  + writeHtmlFooter(FileWriter writer)
```

---

## Call Graph - Key Flows

### **1. Application Startup Flow**
```
pgCompare.main()
    ├─> CommandLineParser.parse()
    │   └─> CommandLineParser.createOptions()
    ├─> ApplicationContext (constructor)
    │   └─> Settings.Props
    ├─> ApplicationContext.initialize()
    │   ├─> LoggingUtils.initialize()
    │   ├─> DatabaseConnectionService.getConnection()
    │   ├─> Settings.setProjectConfig()
    │   │   ├─> SQLExecutionHelper.simpleSelectReturnString()
    │   │   └─> JsonProcessingUtils.mergeJsonObjects()
    │   └─> ValidationUtils.all()
    └─> ApplicationContext.executeAction()
        ├─> [ACTION: init] RepositoryInitializationService.createRepository()
        ├─> [ACTION: discover] DiscoverController.discoverTables()
        ├─> [ACTION: discover] DiscoverController.discoverColumns()
        ├─> [ACTION: compare/check] CompareController.performCompare()
        └─> [ACTION: copy-table] TableController.performCopyTable()
```

### **2. Compare/Check Flow**
```
CompareController.performCompare()
    ├─> RepoController.getTables()
    │   └─> SQLExecutionHelper.simpleSelect()
    ├─> TableController.reconcileTables()
    │   └─> For each table:
    │       ├─> TableController.getTableMap()
    │       │   └─> SQLExecutionHelper.simpleSelect()
    │       ├─> ColumnManagementService.getColumnMapping()
    │       │   └─> SQLExecutionHelper.simpleSelectReturnString()
    │       ├─> ColumnController.getColumnInfo()
    │       │   └─> ColumnMetadataBuilder.build()
    │       ├─> CompareController.reconcileData()
    │       │   ├─> [IF check] DataValidationThread.checkRows()
    │       │   │   ├─> SQLExecutionHelper.simpleSelect()
    │       │   │   └─> DataValidationThread.reCheck()
    │       │   │       ├─> SQLExecutionHelper.simpleSelect()
    │       │   │       ├─> DataProcessingUtils.convertClobToString()
    │       │   │       └─> SQLFixGenerationService.generateFixSQL()
    │       │   │           ├─> SQLFixGenerationService.generateDeleteSQL()
    │       │   │           ├─> SQLFixGenerationService.generateInsertSQL()
    │       │   │           └─> SQLFixGenerationService.generateUpdateSQL()
    │       │   ├─> [IF compare] ThreadManager.createComparison()
    │       │   │   ├─> StagingTableService.createStagingTable()
    │       │   │   ├─> Create threads:
    │       │   │   │   ├─> DataComparisonThread (source)
    │       │   │   │   ├─> DataComparisonThread (target)
    │       │   │   │   ├─> DataLoaderThread (source)
    │       │   │   │   ├─> DataLoaderThread (target)
    │       │   │   │   └─> ObserverThread
    │       │   │   ├─> Start all threads
    │       │   │   ├─> Wait for completion
    │       │   │   └─> StagingTableService.dropStagingTable()
    │       │   └─> ResultProcessor.processResults()
    │       │       ├─> SQLExecutionHelper.simpleUpdate() [mark missing/not equal]
    │       │       └─> SQLExecutionHelper.simpleUpdateReturning() [update counts]
    │       └─> RepoController.completeTableHistory()
    └─> ReportController.createSummary()
        ├─> ReportController.generateCompleteReport()
        └─> HTMLWriterUtils.writeHtmlReport()
```

### **3. Thread Execution Flow**
```
DataComparisonThread.run()
    ├─> initializeRepositoryConnection()
    │   └─> DatabaseConnectionService.getConnection()
    ├─> initializeSourceTargetConnection()
    │   └─> DatabaseConnectionService.getConnection()
    ├─> Execute SQL query
    ├─> For each row:
    │   ├─> HashingUtils.getMd5() [if not using database hash]
    │   ├─> [IF using loader threads] handleLoaderThreadBatch()
    │   │   └─> BlockingQueue.put()
    │   ├─> [IF direct load] handleDirectDatabaseBatch()
    │   │   └─> PreparedStatement.executeLargeBatch()
    │   └─> [IF observer checkpoint] handleObserverCoordination()
    │       ├─> RepoController.dcrUpdateRowCount()
    │       └─> ThreadSync.observerWait()
    ├─> processRemainingRecords()
    │   └─> RepoController.dcrUpdateRowCount()
    └─> cleanupResources()

DataLoaderThread.run()
    └─> Loop until queue empty:
        ├─> BlockingQueue.take()
        └─> SQLExecutionHelper.simpleUpdate() [load to staging table]

ObserverThread.run()
    └─> Loop while not complete:
        ├─> Execute SQL to match rows
        ├─> RepoController.dcrUpdateRowCount()
        ├─> ThreadSync.observerNotify()
        └─> [IF complete] RepoController.vacuumRepo()
```

### **4. Discover Flow**
```
ApplicationContext.executeAction() [ACTION: discover]
    ├─> DiscoverController.discoverTables()
    │   ├─> DatabaseMetadataService.getTables()
    │   │   └─> Execute platform-specific SQL
    │   ├─> For each table:
    │   │   ├─> TableManagementService.saveTable()
    │   │   │   └─> SQLExecutionHelper.simpleUpdateReturningInteger()
    │   │   └─> TableManagementService.saveTableMap()
    │   │       └─> SQLExecutionHelper.simpleUpdate()
    │   └─> SQLExecutionHelper.simpleUpdate() [cleanup]
    └─> DiscoverController.discoverColumns()
        ├─> ColumnDiscoveryService.discoverColumns()
        │   ├─> ColumnDiscoveryService.clearPreviousMappings()
        │   │   └─> SQLExecutionHelper.simpleUpdate()
        │   ├─> ColumnDiscoveryService.discoverColumnsForRole() [target]
        │   │   ├─> SQLExecutionHelper.simpleSelect()
        │   │   └─> ColumnDiscoveryService.loadColumns()
        │   │       ├─> ColumnMetadataUtils.getColumns()
        │   │       │   └─> SQLExecutionHelper.simpleSelect()
        │   │       ├─> ColumnDiscoveryService.findExistingColumn()
        │   │       │   └─> SQLExecutionHelper.simpleSelectReturnInteger()
        │   │       ├─> RepoController.saveTableColumn()
        │   │       │   └─> SQLExecutionHelper.simpleUpdateReturningInteger()
        │   │       └─> RepoController.saveTableColumnMap()
        │   │           └─> SQLExecutionHelper.simpleUpdate()
        │   └─> ColumnDiscoveryService.discoverColumnsForRole() [source]
        │       └─> [Same flow as target]
        └─> DisplayOperations.displayResults()
```

---

## Component Dependency Graph

```
                                    pgCompare (main)
                                         │
                                         ▼
                              CommandLineParser ────> Settings
                                         │
                                         ▼
                               ApplicationContext
                    ┌──────────────────┼──────────────────┐
                    │                  │                  │
                    ▼                  ▼                  ▼
          RepositoryInit      DiscoverController    CompareController
                              TableController       ReportController
                              ColumnController
                                    │                    │
                    ┌───────────────┼────────────────┐   │
                    │               │                │   │
                    ▼               ▼                ▼   ▼
         TableManagementService  ThreadManager  ResultProcessor
         ColumnManagementService      │              │
         ColumnDiscoveryService       │              │
         DatabaseConnectionService    │              │
                                      ▼              │
                    ┌─────────────────┴──────────────┴──────────┐
                    │                                            │
                    ▼                                            ▼
         DataComparisonThread                           ObserverThread
         DataLoaderThread
                    │
                    ▼
         ┌──────────┴──────────┐
         │                     │
         ▼                     ▼
   DatabaseMetadataService  SQLExecutionHelper
   SQLSyntaxService         (core.database)
   SQLFixGenerationService
         │                     │
         └──────────┬──────────┘
                    │
                    ▼
            ┌───────┴───────┐
            │               │
            ▼               ▼
        LoggingUtils    HashingUtils
        ValidationUtils DataProcessingUtils
        FileSystemUtils JsonProcessingUtils
        ColumnMetadataUtils
        DisplayOperations
        HTMLWriterUtils
```

---

## Database Tables Used

### Repository Tables (dc_* tables)
1. **dc_project** - Project configurations
2. **dc_result** - Comparison results summary
3. **dc_table** - Table configurations
4. **dc_table_column** - Column configurations
5. **dc_table_column_map** - Column mappings (source/target)
6. **dc_table_map** - Table mappings (source/target)
7. **dc_table_history** - Table comparison history
8. **dc_source** - Source data staging
9. **dc_target** - Target data staging
10. **stagingtable_*** - Temporary staging tables

---

## Key Design Patterns

### 1. **Repository Pattern**
- `RepoController` provides abstraction for repository operations
- All repository SQL is centralized in `RepoSQLConstants`

### 2. **Service Layer Pattern**
- Business logic separated into service classes
- Each service has a single responsibility

### 3. **Thread Pool Pattern**
- `ThreadManager` manages thread lifecycle
- Uses `BlockingQueue` for thread communication
- `ThreadSync` for thread coordination

### 4. **Builder Pattern**
- `ColumnMetadataBuilder` builds complex column metadata
- Uses method chaining for clarity

### 5. **Factory Pattern**
- `DatabaseConnectionService` creates connections
- `DatabaseMetadataService.DatabasePlatform` enum factory

### 6. **Strategy Pattern**
- Platform-specific SQL via interface constants
- Platform-specific operations via enum methods

---

## Thread Communication

```
┌──────────────────┐         ┌──────────────────┐
│                  │         │                  │
│  DataComparison  │────────>│  BlockingQueue   │
│  Thread (Source) │         │    (Source)      │
│                  │         │                  │
└──────────────────┘         └────────┬─────────┘
                                      │
                                      ▼
                            ┌──────────────────┐
                            │                  │
                            │  DataLoader      │
                            │  Thread (Source) │
                            │                  │
                            └──────────────────┘
                                      │
                                      ▼
                            ┌──────────────────┐
                            │                  │
                            │  Staging Table   │
                            │   (dc_source)    │
                            │                  │
                            └──────────────────┘
                                      │
                    ┌─────────────────┼─────────────────┐
                    │                                   │
                    ▼                                   ▼
        ┌──────────────────┐              ┌──────────────────┐
        │                  │◄────────────>│                  │
        │  Observer Thread │   ThreadSync │  Staging Table   │
        │                  │              │   (dc_target)    │
        └──────────────────┘              └──────────────────┘
                    ▲
                    │
        [Matches rows, updates counts]
```

---

This architecture provides a scalable, multi-threaded solution for comparing large database tables with support for multiple database platforms and comprehensive reporting capabilities.

