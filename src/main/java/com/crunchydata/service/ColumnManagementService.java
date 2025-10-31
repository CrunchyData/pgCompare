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

import com.crunchydata.model.ColumnMetadata;
import com.crunchydata.model.DataComparisonTableColumn;
import com.crunchydata.model.DataComparisonTableMap;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

import static com.crunchydata.config.Settings.Props;
import static com.crunchydata.config.sql.RepoSQLConstants.*;
import static com.crunchydata.controller.ColumnController.getColumnInfo;

/**
 * Service class for managing column-related operations in the repository.
 * This class encapsulates the logic for column creation, retrieval, and management
 * operations, providing a clean interface for column-related database operations.
 * 
 * @author Brian Pace
 */
public class ColumnManagementService {
    
    private static final String THREAD_NAME = "column-management";


    /**
     * Get column mapping for the table.
     *
     * @param connRepo Repository connection
     * @param tid Table ID
     * @return Column mapping JSON string
     * @throws SQLException if database operations fail
     */
    public static String getColumnMapping(Connection connRepo, Integer tid) throws SQLException {
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(tid);
        return SQLExecutionService.simpleSelectReturnString(connRepo, SQL_REPO_DCTABLECOLUMNMAP_FULLBYTID, binds);
    }


    /**
     * Get source column metadata.
     *
     * @param columnMap Column mapping JSON
     * @param dctmSource Source table map
     * @return Column metadata for source
     */
    public static ColumnMetadata getSourceColumnMetadata(JSONObject columnMap, DataComparisonTableMap dctmSource) {
        return getColumnInfo(columnMap, "source", Props.getProperty("source-type"),
                dctmSource.getSchemaName(), dctmSource.getTableName(),
                "database".equals(Props.getProperty("column-hash-method")));
    }

    /**
     * Get target column metadata.
     *
     * @param columnMap Column mapping JSON
     * @param dctmTarget Target table map
     * @param check Whether this is a check operation
     * @return Column metadata for target
     */
    public static ColumnMetadata getTargetColumnMetadata(JSONObject columnMap, DataComparisonTableMap dctmTarget, Boolean check) {
        return getColumnInfo(columnMap, "target", Props.getProperty("target-type"),
                dctmTarget.getSchemaName(), dctmTarget.getTableName(),
                !check && "database".equals(Props.getProperty("column-hash-method")));
    }

    /**
     * Save table column information to the database.
     * 
     * @param conn Database connection
     * @param dctc Table column model to save
     * @return Updated table column model with generated ID
     * @throws SQLException if database operations fail
     */
    public static DataComparisonTableColumn saveTableColumn(Connection conn, DataComparisonTableColumn dctc) throws SQLException {
        validateTableColumnInputs(conn, dctc);
        
        ArrayList<Object> binds = new ArrayList<>();
        binds.add(dctc.getTid());
        binds.add(dctc.getColumnAlias());
        
        Integer cid = SQLExecutionService.simpleUpdateReturningInteger(conn, SQL_REPO_DCTABLECOLUMN_INSERT, binds);
        dctc.setColumnID(cid);
        
        return dctc;
    }

    /**
     * Validate inputs for saveTableColumn method.
     *
     * @param conn Database connection
     * @param dctc Table column model
     */
    private static void validateTableColumnInputs(Connection conn, DataComparisonTableColumn dctc) {
        if (conn == null) {
            throw new IllegalArgumentException("Database connection cannot be null");
        }
        if (dctc == null) {
            throw new IllegalArgumentException("Table column model cannot be null");
        }
        if (dctc.getTid() == null || dctc.getTid() <= 0) {
            throw new IllegalArgumentException("Table ID must be a positive integer");
        }
        if (dctc.getColumnAlias() == null || dctc.getColumnAlias().trim().isEmpty()) {
            throw new IllegalArgumentException("Column alias cannot be null or empty");
        }
    }
    

}
