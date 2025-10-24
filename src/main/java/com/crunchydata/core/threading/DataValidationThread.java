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

package com.crunchydata.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import javax.sql.RowSetMetaData;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.serial.SerialClob;

import com.crunchydata.model.ColumnMetadata;
import com.crunchydata.model.DCTable;
import com.crunchydata.model.DCTableMap;
import com.crunchydata.model.DataCompare;
import com.crunchydata.util.DataUtility;
import com.crunchydata.util.Logging;

import org.json.JSONArray;
import org.json.JSONObject;

import static com.crunchydata.service.DatabaseService.getQuoteChar;
import static com.crunchydata.util.ColumnUtility.createColumnFilterClause;
import static com.crunchydata.util.ColumnUtility.findColumnAlias;
import static com.crunchydata.util.SQLConstantsRepo.*;
import static com.crunchydata.util.Settings.Props;

/**
 * Thread to perform reconciliation checks on rows that are out of sync.
 *
 * @author Brian Pace
 */
public class threadCheck {

    private static final String THREAD_NAME = "check";
    
    // Constants for better maintainability
    private static final int MAX_ROWS_TO_PROCESS = 1000;
    private static final String COMPARE_RESULT_FIELD = "compare_result";
    private static final String IN_SYNC_STATUS = "in-sync";
    private static final String OUT_OF_SYNC_STATUS = "out-of-sync";
    private static final String MISSING_TARGET = "Missing Target";
    private static final String MISSING_SOURCE = "Missing Source";
    private static final String SUCCESS_STATUS = "success";
    private static final String FAILED_STATUS = "failed";

    /**
     * Pulls a list of out-of-sync rows from the repository dc_source and dc_target tables.
     * For each row, calls reCheck where the row is validated against source and target databases.
     *
     * @param repoConn           Repository database connection.
     * @param sourceConn         Source database connection.
     * @param targetConn         Target database connection.
     * @param ciSource           Column metadata from source database.
     * @param ciTarget           Column metadata from target database.
     * @param cid                Identifier for the reconciliation process.
     */
    public static JSONObject checkRows (Connection repoConn, Connection sourceConn, Connection targetConn, DCTable dct, DCTableMap dctmSource, DCTableMap dctmTarget, ColumnMetadata ciSource, ColumnMetadata ciTarget, Integer cid) {
        ArrayList<Object> binds = new ArrayList<>();
        JSONObject result = new JSONObject();
        JSONArray rows = new JSONArray();

        result.put("status", SUCCESS_STATUS);

        PreparedStatement stmt = null;
        ResultSet rs = null;
        
        try {
            stmt = repoConn.prepareStatement(SQL_REPO_SELECT_OUTOFSYNC_ROWS);
            stmt.setObject(1, dct.getTid());
            stmt.setObject(2, dct.getTid());
            rs = stmt.executeQuery();

            int processedRows = 0;
            while (rs.next() && processedRows < MAX_ROWS_TO_PROCESS) {
                DataCompare dcRow = new DataCompare(null,null,null,null,null,null, 0, dct.getBatchNbr());

                dcRow.setTid(dct.getTid());
                dcRow.setTableName(dct.getTableAlias());
                dcRow.setPkHash(rs.getString("pk_hash"));
                dcRow.setPk(rs.getString("pk"));
                dcRow.setCompareResult(COMPARE_RESULT_FIELD);

                // Get Column Info and Mapping
                binds.clear();
                binds.addFirst(dct.getTid());
                JSONObject columnMapping = new JSONObject(SQLService.simpleSelectReturnString(repoConn, SQL_REPO_DCTABLECOLUMNMAP_FULLBYTID, binds));

                int pkColumnCount = 0;
                binds.clear();
                dctmSource.setTableFilter(" ");
                dctmTarget.setTableFilter(" ");

                JSONObject pk = new JSONObject(dcRow.getPk());
                Iterator<String> keys = pk.keys();
                while (keys.hasNext()) {
                    String key = keys.next();
                    String columnAlias = findColumnAlias(columnMapping.getJSONArray("columns"),  key.replace("`","").replace("\"",""),  "source");
                    if (pk.get(key) instanceof String) {
                        String value = pk.getString(key);
                        binds.add(pkColumnCount,value);
                    } else {
                        Integer value = pk.getInt(key);
                        binds.add(pkColumnCount,value);
                    }
                    dctmSource.setTableFilter(dctmSource.getTableFilter() + createColumnFilterClause(repoConn, dct.getTid(), columnAlias, "source", getQuoteChar(Props.getProperty("source-type"))));
                    dctmTarget.setTableFilter(dctmTarget.getTableFilter() + createColumnFilterClause(repoConn, dct.getTid(), columnAlias, "target", getQuoteChar(Props.getProperty("target-type"))));
                    pkColumnCount++;
                }

                Logging.write("info", THREAD_NAME, String.format("Primary Key:  %s (WHERE = '%s')", pk, dctmSource.getTableFilter()));

                JSONObject recheckResult = reCheck(repoConn, sourceConn, targetConn, dctmSource, dctmTarget, ciTarget.pkList, binds, dcRow, cid);

                if ( rows.length() < MAX_ROWS_TO_PROCESS ) {
                    rows.put(recheckResult);
                }
                
                processedRows++;
            }
            
            Logging.write("info", THREAD_NAME, String.format("Processed %d out-of-sync rows for table %s", processedRows, dct.getTableAlias()));
            result.put("data", rows);

        } catch (SQLException e) {
            result.put("status", FAILED_STATUS);
            Logging.write("severe", THREAD_NAME, String.format("SQL error performing check of table %s: %s", dct.getTableAlias(), e.getMessage()));
        } catch (Exception e) {
            result.put("status", FAILED_STATUS);
            StackTraceElement[] stackTrace = e.getStackTrace();
            Logging.write("severe", THREAD_NAME, String.format("Error performing check of table %s at line %s:  %s", dct.getTableAlias(), stackTrace[0].getLineNumber(), e.getMessage()));
        } finally {
            // Ensure resources are properly closed
            try {
                if (rs != null) {
                    rs.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e) {
                Logging.write("warning", THREAD_NAME, String.format("Error closing resources: %s", e.getMessage()));
            }
        }

        return result;
    }

    /**
     * Pulls a list of out-of-sync rows from the repository dc_source and dc_target tables.
     * For each row, calls reCheck where the row is validated against source and target databases.
     *
     * @param repoConn           Repository database connection.
     * @param sourceConn         Source database connection.
     * @param targetConn         Target database connection.
     * @param pkList             Array of primary key columns.
     * @param dcRow              DataCompare object with row to be compared.
     * @param cid                Identifier for the reconciliation process.
     */
    public static JSONObject reCheck (Connection repoConn, Connection sourceConn, Connection targetConn, DCTableMap dctmSource, DCTableMap dctmTarget, String pkList, ArrayList<Object> binds, DataCompare dcRow, Integer cid) {
        JSONArray arr = new JSONArray();
        int columnOutofSync = 0;
        JSONObject rowResult = new JSONObject();

        // Initialize result with default values
        rowResult.put("compareStatus", IN_SYNC_STATUS);
        rowResult.put("compareResult", " ");
        rowResult.put("equal", 0);
        rowResult.put("notEqual", 0);
        rowResult.put("missingSource", 0);
        rowResult.put("missingTarget", 0);

        CachedRowSet sourceRow = SQLService.simpleSelect(sourceConn, dctmSource.getCompareSQL() + dctmSource.getTableFilter(), binds);
        CachedRowSet targetRow = SQLService.simpleSelect(targetConn, dctmTarget.getCompareSQL() + dctmTarget.getTableFilter(), binds);

        try {
            rowResult.put("pk", dcRow.getPk());

            if (sourceRow.size() > 0 && targetRow.size() == 0) {
                rowResult.put("compareStatus", OUT_OF_SYNC_STATUS);
                rowResult.put("compareResult", MISSING_TARGET);
                rowResult.put("missingTarget", 1);
                rowResult.put("result", new JSONArray().put(0, MISSING_TARGET));
            } else if (targetRow.size() > 0 && sourceRow.size() == 0 ) {
                rowResult.put("compareStatus", OUT_OF_SYNC_STATUS);
                rowResult.put("compareResult", MISSING_SOURCE);
                rowResult.put("missingSource", 1);
                rowResult.put("result", new JSONArray().put(0, MISSING_SOURCE));
            } else {
                // Both rows exist, perform detailed comparison
                RowSetMetaData rowMetadata = (RowSetMetaData) sourceRow.getMetaData();
                sourceRow.next();
                targetRow.next();
                
                for (int i = 3; i <= rowMetadata.getColumnCount(); i++) {
                    String column = rowMetadata.getColumnName(i);

                    try {
                        String sourceValue = extractColumnValue(sourceRow, i);
                        String targetValue = extractColumnValue(targetRow, i);

                        if (!sourceValue.equals(targetValue)) {
                            JSONObject col = new JSONObject();
                            String jsonString = "{ source: " + ((sourceValue.equals(" ")) ? "\" \"" : sourceValue) + ", target: " + ((targetValue.equals(" ")) ? "\" \"" : targetValue) + "}";
                            col.put(column, new JSONObject(jsonString));
                            arr.put(columnOutofSync, col);
                            columnOutofSync++;
                        }
                    } catch (Exception e) {
                        StackTraceElement[] stackTrace = e.getStackTrace();
                        Logging.write("severe", THREAD_NAME, String.format("Error comparing column values at line %s: %s",stackTrace[0].getLineNumber(), e.getMessage()));
                        Logging.write("severe", THREAD_NAME, String.format("Error on column %s",column));
                        Logging.write("severe", THREAD_NAME, String.format("Source values:  %s", sourceRow.getString(i)));
                        Logging.write("severe", THREAD_NAME, String.format("Target values:  %s", targetRow.getString(i)));
                    }
                }

                if (columnOutofSync > 0) {
                    rowResult.put("compareStatus", OUT_OF_SYNC_STATUS);
                    rowResult.put("compareResult", arr.toString());
                    rowResult.put("pk", dcRow.getPk());
                    rowResult.put("notEqual", 1);
                    rowResult.put("result", arr);
                }
            }

            // Handle in-sync rows
            if (IN_SYNC_STATUS.equals(rowResult.get("compareStatus"))) {
                rowResult.put("equal", 1);
                removeInSyncRow(repoConn, dcRow);
            } else {
                Logging.write("warning", THREAD_NAME, String.format("Out-of-Sync:  PK = %s; Differences = %s", dcRow.getPk(), rowResult.getJSONArray("result").toString()));
            }

            // Update result counts
            updateResultCounts(repoConn, rowResult, sourceRow, targetRow, cid);

        } catch (SQLException e) {
            Logging.write("severe", THREAD_NAME, String.format("SQL error comparing source and target values: %s", e.getMessage()));
        } catch (Exception e) {
            StackTraceElement[] stackTrace = e.getStackTrace();
            Logging.write("severe", THREAD_NAME, String.format("Error comparing source and target values at line %s:  %s", stackTrace[0].getLineNumber(), e.getMessage()));
        }

        return rowResult;
    }
    
    /**
     * Extracts a column value, handling CLOB types properly.
     */
    private static String extractColumnValue(CachedRowSet rowSet, int columnIndex) throws Exception {
        String value = rowSet.getString(columnIndex);
        
        if (value != null && value.contains("javax.sql.rowset.serial.SerialClob")) {
            return DataUtility.convertClobToString((SerialClob) rowSet.getObject(columnIndex));
        }
        
        return value;
    }
    
    /**
     * Removes in-sync rows from staging tables.
     */
    private static void removeInSyncRow(Connection repoConn, DataCompare dcRow) {
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(0, dcRow.getTid());
        binds.add(1, dcRow.getPkHash());
        binds.add(2, dcRow.getBatchNbr());
        
        SQLService.simpleUpdate(repoConn, SQL_REPO_DCSOURCE_DELETE, binds, true);
        SQLService.simpleUpdate(repoConn, SQL_REPO_DCTARGET_DELETE, binds, true);
    }
    
    /**
     * Updates result counts in the repository.
     */
    private static void updateResultCounts(Connection repoConn, JSONObject rowResult, CachedRowSet sourceRow, CachedRowSet targetRow, Integer cid) {
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(0, rowResult.getInt("equal"));
        binds.add(1, sourceRow.size());
        binds.add(2, targetRow.size());
        binds.add(3, cid);
        
        SQLService.simpleUpdate(repoConn, SQL_REPO_DCRESULT_UPDATE_ALLCOUNTS, binds, true);
    }

}
