/*
 * Copyright 2012-2025 the original author or authors.
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

import com.crunchydata.model.DataComparisonTableColumn;
import com.crunchydata.model.DataComparisonTableColumnMap;
import com.crunchydata.util.LoggingUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.sql.rowset.CachedRowSet;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;

import static com.crunchydata.util.ColumnMetadataUtils.getColumns;
import static com.crunchydata.config.sql.RepoSQLConstants.*;

/**
 * Service class for handling column discovery operations.
 * This class provides optimized methods for discovering and processing
 * database columns from source and target databases.
 * 
 * @author Brian Pace
 * @version 1.0
 */
public class ColumnDiscoveryService {
    
    private static final String THREAD_NAME = "column-discovery";
    private static final String TARGET_ROLE = "target";
    private static final String SOURCE_ROLE = "source";
    
    /**
     * Discover columns from both source and target databases.
     * 
     * @param props Application properties
     * @param pid Project ID
     * @param table Table name filter (empty for all tables)
     * @param connRepo Repository connection
     * @param connSource Source database connection
     * @param connTarget Target database connection
     * @throws SQLException if database operations fail
     */
    public static void discoverColumns(Properties props, Integer pid, String table, 
                                     Connection connRepo, Connection connSource, Connection connTarget) 
                                     throws SQLException {
        
        // Clear previous mappings
        clearPreviousMappings(connRepo, pid, table);
        
        // Discover target columns first
        discoverColumnsForRole(props, pid, table, connRepo, connTarget, TARGET_ROLE);
        
        // Discover source columns
        discoverColumnsForRole(props, pid, table, connRepo, connSource, SOURCE_ROLE);
    }
    
    /**
     * Clear previous column mappings.
     * 
     * @param connRepo Repository connection
     * @param pid Project ID
     * @param table Table name filter
     * @throws SQLException if database operations fail
     */
    private static void clearPreviousMappings(Connection connRepo, Integer pid, String table) throws SQLException {
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(pid);
        if (!table.isEmpty()) {
            binds.add(table);
        }
        
        String sql = table.isEmpty() 
            ? SQL_REPO_DCTABLECOLUMN_DELETEBYPID 
            : SQL_REPO_DCTABLECOLUMN_DELETEBYPIDTABLE;
            
        SQLExecutionService.simpleUpdate(connRepo, sql, binds, true);
    }
    
    /**
     * Discover columns for a specific role (source or target).
     * 
     * @param props Application properties
     * @param pid Project ID
     * @param table Table name filter
     * @param connRepo Repository connection
     * @param connDest Destination database connection
     * @param role Role (source or target)
     * @throws SQLException if database operations fail
     */
    private static void discoverColumnsForRole(Properties props, Integer pid, String table,
                                             Connection connRepo, Connection connDest, String role) 
                                             throws SQLException {
        
        String sql = table.isEmpty() 
            ? SQL_REPO_DCTABLEMAP_SELECTBYPIDORIGIN 
            : SQL_REPO_DCTABLEMAP_SELECTBYPIDORIGINTABLE;
        
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(pid);
        binds.add(role);
        if (!table.isEmpty()) {
            binds.add(table);
        }
        
        try (CachedRowSet crs = SQLExecutionService.simpleSelect(connRepo, sql, binds)) {
            while (crs.next()) {
                loadColumns(props, crs.getInt("tid"), 
                    crs.getString("schema_name"), crs.getString("table_name"), 
                    connRepo, connDest, role, true);
            }
        }
    }
    
    /**
     * Load columns for a specific table.
     * 
     * @param props Application properties
     * @param tid Table ID
     * @param schema Schema name
     * @param tableName Table name
     * @param connRepo Repository connection
     * @param connDest Destination database connection
     * @param destRole Destination role
     * @param populateDCTableColumn Whether to populate DCTableColumn
     * @throws SQLException if database operations fail
     */
    public static void loadColumns(Properties props, Integer tid, String schema, String tableName,
                                 Connection connRepo, Connection connDest, String destRole, 
                                 Boolean populateDCTableColumn) throws SQLException {
        
        String destType = props.getProperty(destRole + "-type");
        int columnCount = 0;
        
        LoggingUtils.write("info", THREAD_NAME,
            String.format("(%s) Performing column discovery on %s for table %s", 
                destRole, destType, tableName));
        
        // Get columns from the database
        JSONArray columns = getColumns(props, connDest, schema, tableName, destRole);
        
        // Process each column
        for (int i = 0; i < columns.length(); i++) {
            JSONObject columnInfo = columns.getJSONObject(i);
            String columnName = columnInfo.getString("columnName");
            
            // Check if column already exists
            Integer cid = findExistingColumn(connRepo, tid, columnName);
            
            // Create or update column record
            DataComparisonTableColumn dtc = createOrUpdateColumn(tid, columnName, cid, populateDCTableColumn, connRepo);
            
            if (dtc.getColumnID() != null) {
                columnCount++;
                createColumnMapping(connRepo, tid, dtc.getColumnID(), destRole, columnInfo);
                LoggingUtils.write("info", THREAD_NAME,
                    String.format("(%s) Discovered Column: %s", destRole, columnName));
            }
        }
        
        LoggingUtils.write("info", THREAD_NAME,
            String.format("(%s) Discovered %d columns for table %s", destRole, columnCount, tableName));
    }
    
    /**
     * Find existing column by table ID and column name.
     * 
     * @param connRepo Repository connection
     * @param tid Table ID
     * @param columnName Column name
     * @return Column ID if found, null otherwise
     * @throws SQLException if database operations fail
     */
    private static Integer findExistingColumn(Connection connRepo, Integer tid, String columnName) 
            throws SQLException {
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(tid);
        binds.add(columnName);
        
        return SQLExecutionService.simpleSelectReturnInteger(connRepo, SQL_REPO_DCTABLECOLUMN_SELECTBYTIDALIAS, binds);
    }
    
    /**
     * Create or update a column record.
     * 
     * @param tid Table ID
     * @param columnName Column name
     * @param cid Existing column ID
     * @param populateDCTableColumn Whether to populate DCTableColumn
     * @param connRepo Repository connection
     * @return DCTableColumn object
     * @throws SQLException if database operations fail
     */
    private static DataComparisonTableColumn createOrUpdateColumn(Integer tid, String columnName, Integer cid,
                                                                  Boolean populateDCTableColumn, Connection connRepo)
                                                    throws SQLException {
        DataComparisonTableColumn dtc = new DataComparisonTableColumn();
        dtc.setTid(tid);
        dtc.setColumnAlias(columnName.toLowerCase());
        
        if (cid == null) {
            if (populateDCTableColumn) {
                dtc = com.crunchydata.controller.RepoController.saveTableColumn(connRepo, dtc);
            } else {
                LoggingUtils.write("warning", THREAD_NAME,
                    String.format("Skipping column since no column alias found for %s", columnName));
            }
        } else {
            dtc.setColumnID(cid);
        }
        
        return dtc;
    }
    
    /**
     * Create column mapping record.
     * 
     * @param connRepo Repository connection
     * @param tid Table ID
     * @param columnID Column ID
     * @param destRole Destination role
     * @param columnInfo Column information JSON
     * @throws SQLException if database operations fail
     */
    private static void createColumnMapping(Connection connRepo, Integer tid, Integer columnID,
                                         String destRole, JSONObject columnInfo) throws SQLException {
        DataComparisonTableColumnMap dctcm = new DataComparisonTableColumnMap();
        
        dctcm.setTid(tid);
        dctcm.setColumnID(columnID);
        dctcm.setColumnOrigin(destRole);
        dctcm.setColumnName(columnInfo.getString("columnName"));
        dctcm.setDataType(columnInfo.getString("dataType"));
        dctcm.setDataClass(columnInfo.getString("dataClass"));
        dctcm.setDataLength(columnInfo.getInt("dataLength"));
        dctcm.setNumberPrecision(columnInfo.getInt("dataPrecision"));
        dctcm.setNumberScale(columnInfo.getInt("dataScale"));
        dctcm.setColumnNullable(columnInfo.getBoolean("nullable"));
        dctcm.setColumnPrimaryKey(columnInfo.getBoolean("primaryKey"));
        dctcm.setSupported(columnInfo.getBoolean("supported"));
        dctcm.setPreserveCase(columnInfo.getBoolean("preserveCase"));
        
        com.crunchydata.controller.RepoController.saveTableColumnMap(connRepo, dctcm);
    }
}
