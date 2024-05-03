package com.crunchydata.services;

import com.crunchydata.model.DataCompare;
import com.crunchydata.util.Logging;
import com.crunchydata.util.ThreadSync;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.crunchydata.util.Settings.Props;

public class dbLoader extends Thread  {
    BlockingQueue<DataCompare[]> q;
    Integer instanceNumber;
    String stagingTable;
    String targetType;
    Integer threadNumber;
    String threadName;

    ThreadSync ts;

    public dbLoader(Integer threadNumber, Integer instanceNumber, String targetType, BlockingQueue<DataCompare[]> q, String stagingTable, ThreadSync ts) {
        this.q = q;
        this.instanceNumber = instanceNumber;
        this.stagingTable = stagingTable;
        this.targetType = targetType;
        this.threadNumber = threadNumber;
        this.ts = ts;
    }

    public void run() {
        threadName = "loader-" + targetType + "-t" + threadNumber + "-i" + instanceNumber;
        Logging.write("info", threadName, "Start repository loader thread");

        Connection repoConn = null;
        boolean stillLoading = true;
        PreparedStatement stmtLoad = null;
        int totalRows = 0;

        try {
            /////////////////////////////////////////////////
            // Connect to Repository
            /////////////////////////////////////////////////
            Logging.write("info", threadName, "Connecting to repository database");

            repoConn = dbPostgres.getConnection(Props, "repo", "reconcile");
            if (repoConn == null) {
                Logging.write("severe", threadName, "Cannot connect to repository database");
                System.exit(1);
            }
            repoConn.setAutoCommit(false);

            String sqlLoad = "INSERT INTO " + stagingTable + " (pk_hash, column_hash, pk) VALUES (?,?,(?)::jsonb)";
            repoConn.setAutoCommit(false);
            stmtLoad = repoConn.prepareStatement(sqlLoad);

            while (stillLoading) {

                DataCompare[] dc = q.poll(1, TimeUnit.SECONDS);

                if (dc != null && dc.length > 0) {
                    for (int i = 0; i < dc.length; i++) {
                        if (dc[i] != null && dc[i].getPk() != null) {
                            totalRows++;
                            stmtLoad.setString(1, dc[i].getPkHash());
                            stmtLoad.setString(2, dc[i].getColumnHash());
                            stmtLoad.setString(3, dc[i].getPk());
                            stmtLoad.addBatch();
                            stmtLoad.clearParameters();
                        } else {
                            break;
                        }
                    }
                    stmtLoad.executeBatch();
                    stmtLoad.clearBatch();
                    repoConn.commit();
                }

                if (ts.sourceComplete && ts.targetComplete) {
                    stillLoading = false;
                }
            }

            Logging.write("info", threadName, "Loader thread complete.  Rows inserted: " + totalRows);

            stmtLoad.close();
            repoConn.close();

            stmtLoad = null;
            repoConn = null;

            ts.loaderThreadComplete++;

        } catch( SQLException e) {
            Logging.write("severe", threadName, "Database error " + e.getMessage());
        } catch (Exception e) {
            Logging.write("severe", threadName, "Error in loader thread " + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                if (stmtLoad != null) {
                    stmtLoad.close();
                }

                if (repoConn != null) {
                    repoConn.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
