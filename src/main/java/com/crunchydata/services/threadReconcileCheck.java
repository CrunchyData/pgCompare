/*
 * Copyright 2012-2024 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.crunchydata.services;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Iterator;
import javax.sql.RowSetMetaData;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.serial.SerialClob;

import com.crunchydata.model.ColumnMetadata;
import com.crunchydata.model.DataCompare;
import com.crunchydata.util.DataUtility;
import com.crunchydata.util.Logging;

import org.json.JSONArray;
import org.json.JSONObject;

import static com.crunchydata.util.SQLConstants.SQL_REPO_DCRESULT_UPDATE_ALLCOUNTS;
import static com.crunchydata.util.SQLConstants.SQL_REPO_SELECT_OUTOFSYNC_ROWS;

/**
 * Thread to perform reconciliation checks on rows that are out of sync.
 *
 * @author Brian Pace
 */
public class threadReconcileCheck {

    private static final String THREAD_NAME = "ReconcileCheck";

    /**
     * Pulls a list of out-of-sync rows from the repository dc_source and dc_target tables.
     * For each row, calls reCheck where the row is validated against source and target databases.
     *
     * @param repoConn           Repository database connection.
     * @param sqlSource          SQL to use on the source database.
     * @param sqlTarget          SQL to use on the target database.
     * @param sourceConn         Source database connection.
     * @param targetConn         Target database connection.
     * @param sourceTable        Name of the table on the source database.
     * @param targetTable        Name of the table on the target database.
     * @param ciSource           Column metadata from source database.
     * @param ciTarget           Column metadata from target database.
     * @param batchNbr           Batch number identifier.
     * @param cid                Identifier for the reconciliation process.
     */
    public static void checkRows (Connection repoConn, String sqlSource, String sqlTarget, Connection sourceConn, Connection targetConn, String sourceTable, String targetTable, ColumnMetadata ciSource, ColumnMetadata ciTarget, Integer batchNbr, Integer cid) {
        ArrayList<Object> binds = new ArrayList<>();
        JSONObject result = new JSONObject();
        StringBuilder tableFilter;

        result.put("status","failed");
        result.put("compareStatus","failed");

        try {
            PreparedStatement stmt = repoConn.prepareStatement(SQL_REPO_SELECT_OUTOFSYNC_ROWS);
            stmt.setObject(1, sourceTable);
            stmt.setObject(2, targetTable);
            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                DataCompare dcRow = new DataCompare(null,null,null,null,null, 0, batchNbr);
                dcRow.setTableName(targetTable);
                dcRow.setPkHash(rs.getString("pk_hash"));
                dcRow.setPk(rs.getString("pk"));
                dcRow.setCompareResult("compare_result");
                int pkColumns = 0;
                binds.clear();
                tableFilter = new StringBuilder(" AND ");
                JSONObject pk = new JSONObject(dcRow.getPk());
                Iterator<String> keys = pk.keys();
                while (keys.hasNext()) {
                    String key = keys.next();
                    if (pk.get(key) instanceof String) {
                        String value = pk.getString(key);
                        binds.add(pkColumns,value);
                    } else {
                        Integer value = pk.getInt(key);
                        binds.add(pkColumns,value);
                    }
                    tableFilter.append(key).append(" = ? AND ");
                    pkColumns++;
                }
                tableFilter = new StringBuilder(tableFilter.substring(0, tableFilter.length() - 5));
                Logging.write("info", THREAD_NAME, String.format("Primary Key:  %s", pk));

                reCheck(repoConn, sourceConn, targetConn, sqlSource, sqlTarget, tableFilter.toString(), ciTarget.pkList, binds, dcRow, cid);

            }

            rs.close();
            stmt.close();
        } catch (Exception e) {
            Logging.write("severe", THREAD_NAME, String.format("Error performing check of table %s:  %s", targetTable, e.getMessage()));
        }

    }

    /**
     * Pulls a list of out-of-sync rows from the repository dc_source and dc_target tables.
     * For each row, calls reCheck where the row is validated against source and target databases.
     *
     * @param repoConn           Repository database connection.
     * @param sourceConn         Source database connection.
     * @param targetConn         Target database connection.
     * @param sourceSQL          SQL to use on the source database.
     * @param targetSQL          SQL to use on the target database.
     * @param tableFilter        Filter (where predicate) to be applied to the source or target SQL.
     * @param pkList             Array of primary key columns.
     * @param dcRow              DataCompare object with row to be compared.
     * @param cid                Identifier for the reconciliation process.
     */
    public static void reCheck (Connection repoConn, Connection sourceConn, Connection targetConn, String sourceSQL, String targetSQL, String tableFilter, String pkList, ArrayList<Object> binds, DataCompare dcRow, Integer cid) {
        JSONArray arr = new JSONArray();
        int columnOutofSync = 0;
        JSONObject rowResult = new JSONObject();

        rowResult.put("compareStatus","in-sync");
        rowResult.put("equal",0);
        rowResult.put("notEqual",0);
        rowResult.put("missingSource",0);
        rowResult.put("missingTarget",0);

        CachedRowSet sourceRow = dbCommon.simpleSelect(sourceConn, sourceSQL + tableFilter, binds);
        CachedRowSet targetRow = dbCommon.simpleSelect(targetConn, targetSQL + tableFilter, binds);

        try {
            if (sourceRow.size() > 0 && targetRow.size() == 0) {
                rowResult.put("compareStatus", "out-of-sync");
                rowResult.put("missingTarget", 1);
                rowResult.put("result", new JSONArray().put(0, "Missing Target"));
            } else if (targetRow.size() > 0 && sourceRow.size() == 0 ) {
                rowResult.put("compareStatus", "out-of-sync");
                rowResult.put("missingSource", 1);
                rowResult.put("result", new JSONArray().put(0, "Missing Source"));
            } else {

                RowSetMetaData rowMetadata = (RowSetMetaData) sourceRow.getMetaData();
                sourceRow.next();
                targetRow.next();
                for (int i = 2; i <= rowMetadata.getColumnCount(); i++) {
                    String column = rowMetadata.getColumnName(i);
                    String sourceValue = (sourceRow.getString(i).contains("javax.sql.rowset.serial.SerialClob")) ? DataUtility.convertClobToString((SerialClob) sourceRow.getObject(i)) : sourceRow.getString(i);
                    String targetValue = (targetRow.getString(i).contains("javax.sql.rowset.serial.SerialClob")) ? DataUtility.convertClobToString((SerialClob) targetRow.getObject(i)) : targetRow.getString(i);

                    if (!sourceValue.equals(targetValue)) {
                        JSONObject col = new JSONObject();
                        String jsonString = "{ source: " + sourceValue + ", target: " + ((targetValue.equals(" ")) ? "\" \"" : targetValue) + "}";
                        col.put(column, new JSONObject(jsonString));
                        arr.put(columnOutofSync, col);
                        columnOutofSync++;
                    }
                }

                if (columnOutofSync > 0) {
                    rowResult.put("compareStatus", "out-of-sync");
                    rowResult.put("notEqual", 1);
                    rowResult.put("result", arr);
                }

            }

            if (rowResult.get("compareStatus").equals("in-sync")) {
                rowResult.put("equal",1);
                binds.clear();
                binds.add(0,dcRow.getTableName());
                binds.add(1,dcRow.getPkHash());
                binds.add(2, dcRow.getBatchNbr());
                dbCommon.simpleUpdate(repoConn, "DELETE FROM dc_source WHERE lower(table_name)=lower(?) AND pk_hash=? AND batch_nbr=?", binds, true);
                dbCommon.simpleUpdate(repoConn, "DELETE FROM dc_target WHERE lower(table_name)=lower(?) AND pk_hash=? AND batch_nbr=?", binds, true);
            } else {
                Logging.write("warning", THREAD_NAME, String.format("Out-of-Sync:  PK = %s; Differences = %s", dcRow.getPk(), rowResult.getJSONArray("result").toString()));
            }

            binds.clear();
            binds.add(0,rowResult.getInt("equal"));
            binds.add(1,sourceRow.size());
            binds.add(2,targetRow.size());
            binds.add(3,cid);
            dbCommon.simpleUpdate(repoConn, SQL_REPO_DCRESULT_UPDATE_ALLCOUNTS, binds, true);


        } catch (Exception e) {
            Logging.write("severe", THREAD_NAME, String.format("Error comparing source and target values:  %s", e.getMessage()));
        }

    }

}
