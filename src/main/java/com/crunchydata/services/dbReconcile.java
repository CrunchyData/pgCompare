package com.crunchydata.services;

import com.crunchydata.controller.RepoController;
import com.crunchydata.model.DataCompare;
import com.crunchydata.util.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;

import static com.crunchydata.util.SecurityUtility.getMd5;
import static com.crunchydata.util.Settings.Props;

public class dbReconcile extends Thread {
    String modColumn;
    Integer parallelDegree;
    String schemaName;
    String tableName;
    String sql;
    String tableFilter;
    String targetType;
    Integer threadNumber;
    String threadName;

    Integer nbrColumns;
    Integer nbrPKColumns;
    Integer cid;
    Integer batchNbr;

    ThreadSync ts;

    String pkList;

    Boolean sameRDBMSOptimization;

    public dbReconcile(Integer threadNumber, String targetType, String sql, String tableFilter, String modColumn, Integer parallelDegree, String schemaName, String tableName, Integer nbrColumns, Integer nbrPKColumns, Integer cid, ThreadSync ts, String pkList, Boolean sameRDBMSOptimization, Integer batchNbr) {
        this.modColumn = modColumn;
        this.parallelDegree = parallelDegree;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.sql = sql;
        this.tableFilter = tableFilter;
        this.targetType = targetType;
        this.threadNumber = threadNumber;
        this.nbrColumns = nbrColumns;
        this.nbrPKColumns = nbrPKColumns;
        this.cid = cid;
        this.ts = ts;
        this.pkList = pkList;
        this.sameRDBMSOptimization = sameRDBMSOptimization;
        this.batchNbr = batchNbr;
    }

    public void run() {

        threadName = "reconcile-"+targetType+"-"+threadNumber;
        Logging.write("info", threadName, "Start database reconcile thread");

        /////////////////////////////////////////////////
        // Connect to Repository
        /////////////////////////////////////////////////
        Connection repoConn;
        Logging.write("info", threadName, "Connecting to repository database");
        repoConn = dbPostgres.getConnection(Props,"repo", "reconcile");
        if ( repoConn == null) {
            Logging.write("severe", threadName, "Cannot connect to repository database");
            System.exit(1);
        }
        try { repoConn.setAutoCommit(false); } catch (Exception e) {}

        /////////////////////////////////////////////////
        // Connect to Source/Target
        /////////////////////////////////////////////////
        Connection conn;

        Logging.write("info", threadName, "Connecting to " + targetType + " database");
        if (Props.getProperty(targetType + "-type").equals("oracle")) {
            conn = dbOracle.getConnection(Props,targetType);
        } else {
            conn = dbPostgres.getConnection(Props,targetType, "reconcile");
            try { conn.setAutoCommit(false); } catch (Exception e) {}
        }
        if ( conn == null) {
            Logging.write("severe", threadName, "Cannot connect to " + targetType + " database");
            System.exit(1);
        }

        /////////////////////////////////////////////////
        // Load Reconcile Data
        /////////////////////////////////////////////////
        ResultSet rs;
        PreparedStatement stmt;
        ArrayList<DataCompare> dataCompareList = new ArrayList<>();
        int cntRecord = 0;
        int totalRows = 0;

        if ( parallelDegree > 1 && !modColumn.isEmpty()) {
            sql += " AND mod(" + modColumn + "," + parallelDegree +")="+threadNumber;
        }

        if (!pkList.isEmpty()) {
            sql += " ORDER BY " + pkList;
        }

        try {
            conn.setAutoCommit(false);
            stmt = conn.prepareStatement(sql);
            stmt.setFetchSize(Integer.parseInt(Props.getProperty("batch-fetch-size")));
            rs = stmt.executeQuery();
            int loadRowCount = 10000;
            int observerRowCount = 10000;
            boolean firstPass = true;

            rs.setFetchSize(Integer.parseInt(Props.getProperty("batch-fetch-size")));

            while (rs.next()) {
                StringBuilder columnValue = new StringBuilder();

                if (! sameRDBMSOptimization) {
                    for (int i = 3; i < nbrColumns + 3 - nbrPKColumns; i++) {
                        columnValue.append(rs.getString(i));
                    }
                }
                dataCompareList.add(new DataCompare(tableName, (sameRDBMSOptimization) ? rs.getString("PK_HASH") : getMd5(rs.getString("PK_HASH")),  (sameRDBMSOptimization) ? columnValue.toString() : getMd5(columnValue.toString()), rs.getString("PK"),null, threadNumber, batchNbr));
                cntRecord++;
                totalRows++;

                if (totalRows % loadRowCount == 0) {
                    RepoController.loadDataCompare(repoConn, targetType, dataCompareList);
                    dataCompareList.clear();
                    Logging.write("info", threadName, "Loaded " + totalRows + " rows");
                }

                if (totalRows % observerRowCount == 0) {
                    if (firstPass || Boolean.parseBoolean(Props.getProperty("observer-throttle"))) {
                        firstPass = false;
                        Logging.write("info", threadName, "Wait for Observer");
                        RepoController.dcrUpdateRowCount(repoConn, targetType, cid, cntRecord);
                        repoConn.commit();
                        cntRecord=0;
                        ts.ObserverWait();
                        Logging.write("info", threadName, "Cleared by Observer");
                    } else {
                        Logging.write("info", threadName, "Pause for Observer");
                        Thread.sleep(1000);
                    }
                }

                if (!firstPass) {
                    loadRowCount = Integer.parseInt(Props.getProperty("batch-load-size"));
                    observerRowCount = Integer.parseInt(Props.getProperty("observer-throttle-size"));
                }

            }

            // Loading Remaining
            if (cntRecord > 0) {
                RepoController.loadDataCompare(repoConn, targetType, dataCompareList);
                RepoController.dcrUpdateRowCount(repoConn, targetType, cid, cntRecord);
                dataCompareList.clear();
            }

            stmt.close();

            Logging.write("info", threadName, "Complete. Total rows loaded: " + totalRows);

        } catch (Exception e) {
            Logging.write("severe", threadName, "Error loading hash rows: " + e.getMessage());
        }

        if ( targetType.equals("source")) {
            ts.sourceComplete = true;
        } else {
            ts.targetComplete = true;
        }


        /////////////////////////////////////////////////
        // Close Connections
        /////////////////////////////////////////////////
        try { repoConn.close(); } catch (Exception e) {}
        try { conn.close(); } catch (Exception e) {}

    }
}
