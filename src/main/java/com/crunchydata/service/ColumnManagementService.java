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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

import static com.crunchydata.config.sql.RepoSQLConstants.*;

/**
 * Service class for managing column-related operations in the repository.
 * This class encapsulates the logic for column creation, retrieval, and management
 * operations, providing a clean interface for column-related database operations.
 * 
 * @author Brian Pace
 * @version 1.0
 */
public class ColumnManagementService {
    
    private static final String THREAD_NAME = "column-management";
    
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
