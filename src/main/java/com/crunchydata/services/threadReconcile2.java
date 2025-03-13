package com.crunchydata.services;

import com.crunchydata.controller.RepoController;
import com.crunchydata.models.ColumnMetadata;
import com.crunchydata.models.DCTable;
import com.crunchydata.models.DCTableMap;
import com.crunchydata.models.DataCompare;
import com.crunchydata.util.Logging;
import com.crunchydata.util.ThreadSync;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;

import static com.crunchydata.util.HashUtility.getMd5;

public class threadReconcile2 extends Thread {
    // Existing fields unchanged...

    // Cached properties for performance
    private final int batchCommitSize;
    private final int batchFetchSize;
    private final int batchProgressReportSize;
    private final int observerThrottleSize;
    private final boolean databaseSort;
    private final boolean observerThrottle;
    private final boolean useLoaderThreads;

    private final Integer tid;
    private final Integer batchNbr;
    private final Integer cid;
    private final String modColumn;
    private final Integer nbrColumns;
    private final Integer parallelDegree;
    private final String pkList;
    private BlockingQueue<DataCompare[]> q;
    private String sql;
    private final String stagingTable;
    private final String targetType;
    private final Integer threadNumber;
    private final ThreadSync ts;
    private final Boolean useDatabaseHash;
    private Properties Props;
    private final Integer totalRows = 0;

    public threadReconcile2(Properties props, Integer threadNumber, DCTable dct, DCTableMap dctm, ColumnMetadata cm,
                            Integer cid, ThreadSync ts, Boolean useDatabaseHash, String stagingTable,
                            BlockingQueue<DataCompare[]> q) {
        // Existing constructor logic...
        this.Props = props;
        this.q = q;
        this.modColumn = dctm.getModColumn();
        this.parallelDegree = dct.getParallelDegree();
        this.sql = dctm.getCompareSQL();
        this.targetType = dctm.getDestType();
        this.threadNumber = threadNumber;
        this.nbrColumns = cm.getNbrColumns();
        this.tid = dct.getTid();
        this.cid = cid;
        this.ts = ts;
        this.pkList = cm.getPkList();
        this.useDatabaseHash = useDatabaseHash;
        this.batchNbr = dct.getBatchNbr();
        this.stagingTable = stagingTable;

        // Cache properties
        this.batchCommitSize = Integer.parseInt(props.getProperty("batch-commit-size"));
        this.batchFetchSize = Integer.parseInt(props.getProperty("batch-fetch-size"));
        this.batchProgressReportSize = Integer.parseInt(props.getProperty("batch-progress-report-size"));
        this.observerThrottleSize = Integer.parseInt(props.getProperty("observer-throttle-size"));
        this.databaseSort = Boolean.parseBoolean(props.getProperty("database-sort"));
        this.observerThrottle = Boolean.parseBoolean(props.getProperty("observer-throttle"));
        this.useLoaderThreads = Integer.parseInt(props.getProperty("loader-threads")) > 0;
    }

    @Override
    public void run() {
        String threadName = String.format("Reconcile-%s-c%s-t%s", targetType, cid, threadNumber);
        Logging.write("info", threadName, String.format("(%s) Start database reconcile thread", targetType));

        int totalRows = 0;
        boolean firstPass = true;
        DecimalFormat formatter = new DecimalFormat("#,###");
        StringBuilder columnValue = new StringBuilder(1024); // Pre-size based on expected data

        // Precompute SQL with parallel degree and sorting
        if (parallelDegree > 1 && !modColumn.isEmpty()) {
            sql += " AND mod(" + modColumn + "," + parallelDegree + ")=" + threadNumber;
        }
        if (!pkList.isEmpty() && databaseSort) {
            sql += " ORDER BY " + pkList;
        }

        try (Connection repoConn = dbPostgres.getConnection(Props, "repo", "reconcile")) {
            if (repoConn == null) {
                Logging.write("severe", threadName, String.format("(%s) Cannot connect to repository database", targetType));
                System.exit(1);
            }
            repoConn.setAutoCommit(false);

            Logging.write("info", threadName, String.format("(%s) Connecting to database", targetType));
            try (Connection conn = getConnectionForType(Props, targetType)) {
                if (conn == null) {
                    Logging.write("severe", threadName, String.format("(%s) Cannot connect to database", targetType));
                    System.exit(1);
                }

                try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                    stmt.setFetchSize(batchFetchSize);
                    try (ResultSet rs = stmt.executeQuery()) {
                        RepoController rpc = new RepoController();
                        DataCompare[] dc = useLoaderThreads ? new DataCompare[batchCommitSize] : null;
                        List<Object[]> batchRows = useLoaderThreads ? null : new ArrayList<>(batchCommitSize);
                        String sqlLoad = "INSERT INTO " + stagingTable + " (tid, pk_hash, column_hash, pk) VALUES (?,?,?,(?)::jsonb)";
                        try (PreparedStatement stmtLoad = useLoaderThreads ? null : repoConn.prepareStatement(sqlLoad)) {
                            int cntRecord = 0;

                            while (rs.next()) {
                                columnValue.setLength(0);
                                appendColumnValues(rs, columnValue);

                                if (useLoaderThreads) {
                                    dc[cntRecord] = buildDataCompare(rs, columnValue);
                                    if (++cntRecord == batchCommitSize) {
                                        enqueueBatch(dc, q, threadName);
                                        dc = new DataCompare[batchCommitSize]; // Reuse array could be optimized further
                                        cntRecord = 0;
                                    }
                                } else {
                                    batchRows.add(buildBatchRow(rs, columnValue));
                                    if (++cntRecord == batchCommitSize) {
                                        executeBatch(stmtLoad, batchRows, repoConn);
                                        batchRows.clear();
                                        cntRecord = 0;
                                    }
                                }

                                totalRows++;
                                logProgress(threadName, totalRows, formatter);
                                handleObserverThrottle(firstPass, rpc, repoConn, threadName, cntRecord);
                                if (!firstPass) {
                                    firstPass = false;
                                }
                            }

                            // Handle remaining rows
                            if (cntRecord > 0) {
                                if (useLoaderThreads) {
                                    DataCompare[] finalBatch = new DataCompare[cntRecord];
                                    System.arraycopy(dc, 0, finalBatch, 0, cntRecord);
                                    q.put(finalBatch);
                                } else {
                                    executeBatch(stmtLoad, batchRows, repoConn);
                                }
                                rpc.dcrUpdateRowCount(repoConn, targetType, cid, cntRecord);
                            }

                            Logging.write("info", threadName, String.format("(%s) Complete. Total rows loaded: %s", targetType, formatter.format(totalRows)));
                        }

                        if (useLoaderThreads) {
                            waitForQueueToEmpty(q, threadName);
                        }

                        markThreadComplete(ts, targetType);
                    }
                }
            }
        } catch (SQLException e) {
            logException(threadName, "Database error", e);
        } catch (Exception e) {
            logException(threadName, "Error in reconciliation thread", e);
        }
    }

    private Connection getConnectionForType(Properties props, String targetType) throws SQLException {
        switch (props.getProperty(targetType + "-type")) {
            case "oracle": return dbOracle.getConnection(props, targetType);
            case "mariadb": return dbMariaDB.getConnection(props, targetType);
            case "mysql": return dbMySQL.getConnection(props, targetType);
            case "mssql": return dbMSSQL.getConnection(props, targetType);
            case "db2": return dbDB2.getConnection(props, targetType);
            default: return dbPostgres.getConnection(props, targetType, "reconcile");
        }
    }

    private void appendColumnValues(ResultSet rs, StringBuilder columnValue) throws SQLException {
        if (!useDatabaseHash) {
            for (int i = 3; i < nbrColumns + 3; i++) {
                columnValue.append(rs.getString(i));
            }
        } else {
            columnValue.append(rs.getString(3));
        }
    }

    private DataCompare buildDataCompare(ResultSet rs, StringBuilder columnValue) throws SQLException {
        String pkHash = useDatabaseHash ? rs.getString("PK_HASH") : getMd5(rs.getString("PK_HASH"));
        String columnHash = useDatabaseHash ? columnValue.toString() : getMd5(columnValue.toString());
        String pk = rs.getString("PK").replace(",}", "}");
        return new DataCompare(tid, null, pkHash, columnHash, pk, null, threadNumber, batchNbr);
    }

    private Object[] buildBatchRow(ResultSet rs, StringBuilder columnValue) throws SQLException {
        return new Object[] {
                tid,
                useDatabaseHash ? rs.getString("PK_HASH") : getMd5(rs.getString("PK_HASH")),
                useDatabaseHash ? columnValue.toString() : getMd5(columnValue.toString()),
                rs.getString("PK").replace(",}", "}")
        };
    }

    private void executeBatch(PreparedStatement stmtLoad, List<Object[]> batchRows, Connection repoConn) throws SQLException {
        for (Object[] row : batchRows) {
            stmtLoad.setInt(1, (Integer) row[0]);
            stmtLoad.setString(2, (String) row[1]);
            stmtLoad.setString(3, (String) row[2]);
            stmtLoad.setString(4, (String) row[3]);
            stmtLoad.addBatch();
        }
        stmtLoad.executeLargeBatch();
        repoConn.commit();
    }

    private void enqueueBatch(DataCompare[] dc, BlockingQueue<DataCompare[]> q, String threadName) throws InterruptedException {
        while (q.size() >= 100) {
            Logging.write("info", threadName, String.format("(%s) Waiting for Queue space", targetType));
            Thread.sleep(100); // Reduced sleep time for faster polling
        }
        q.put(dc);
    }

    private void logProgress(String threadName, int totalRows, DecimalFormat formatter) {
        if (totalRows % batchProgressReportSize == 0) {
            Logging.write("info", threadName, String.format("(%s) Loaded %s rows", targetType, formatter.format(totalRows)));
        }
    }

    private void handleObserverThrottle(boolean firstPass, RepoController rpc, Connection repoConn, String threadName, int cntRecord) throws SQLException, InterruptedException {
        if ((firstPass || observerThrottle) && totalRows % observerThrottleSize == 0) {
            Logging.write("info", threadName, String.format("(%s) Wait for Observer", targetType));
            rpc.dcrUpdateRowCount(repoConn, targetType, cid, cntRecord);
            repoConn.commit();
            setWaitingFlag(ts, targetType, true);
            ts.observerWait();
            setWaitingFlag(ts, targetType, false);
            Logging.write("info", threadName, String.format("(%s) Cleared by Observer", targetType));
        }
    }

    private void waitForQueueToEmpty(BlockingQueue<DataCompare[]> q, String threadName) throws InterruptedException {
        while (!q.isEmpty()) {
            Logging.write("info", threadName, String.format("(%s) Waiting for message queue to empty", targetType));
            Thread.sleep(100); // Reduced sleep time
        }
    }

    private void setWaitingFlag(ThreadSync ts, String targetType, boolean value) {
        if ("source".equals(targetType)) {
            ts.sourceWaiting = value;
        } else {
            ts.targetWaiting = value;
        }
    }

    private void markThreadComplete(ThreadSync ts, String targetType) {
        if ("source".equals(targetType)) {
            ts.sourceComplete = true;
        } else {
            ts.targetComplete = true;
        }
    }

    private void logException(String threadName, String message, Exception e) {
        StackTraceElement[] stackTrace = e.getStackTrace();
        Logging.write("severe", threadName, String.format("(%s) %s at line %s: %s", targetType, message,
                stackTrace.length > 0 ? stackTrace[0].getLineNumber() : "unknown", e.getMessage()));
    }
}