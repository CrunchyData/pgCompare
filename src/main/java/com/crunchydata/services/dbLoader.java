package com.crunchydata.services;

import com.crunchydata.model.DataCompare;
import com.crunchydata.util.Logging;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.concurrent.BlockingQueue;

import static com.crunchydata.util.Settings.Props;

public class dbLoader extends Thread  {
    BlockingQueue<DataCompare[]> q;
    Integer instanceNumber;
    String stagingTable;
    String targetType;
    Integer threadNumber;
    String threadName;

    public dbLoader(Integer threadNumber, Integer instanceNumber, String targetType, BlockingQueue<DataCompare[]> q, String stagingTable) {
        this.q = q;
        this.instanceNumber = instanceNumber;
        this.stagingTable = stagingTable;
        this.targetType = targetType;
        this.threadNumber = threadNumber;
    }

    public void run() {
        threadName = "loader-"+targetType+"-"+threadNumber+"-"+instanceNumber;
        Logging.write("info", threadName, "Start repository loader thread");

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
        try { repoConn.setAutoCommit(false); } catch (Exception e) {
            // do nothing
        }

        while(true) {
            try {
                String sqlLoad = "INSERT INTO " + stagingTable + " (pk_hash, column_hash, pk) VALUES (?,?,(?)::jsonb)";
                repoConn.setAutoCommit(false);
                PreparedStatement stmtLoad = repoConn.prepareStatement(sqlLoad);

//                Logging.write("info", threadName, "Reading Queue; size=" + q.size());
                DataCompare[] dc = q.take();
//                Logging.write("info", threadName, "Processing Queue; size=" + q.size());

                for (int i = 0; i < dc.length; i++) {
                    if (!dc[i].getPk().isEmpty()) {
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

                Thread.sleep(1000);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

}
