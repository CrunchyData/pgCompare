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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.crunchydata.util.SQLConstantsRepo.*;

/**
 * Utility class for creating repository schema, tables, indexes, and constraints.
 * This class provides methods to execute SQL DDL statements for repository setup.
 *
 * @author Brian Pace
 */
public class dbRepository {

    /**
     * Creates the repository schema, tables, indexes, and constraints in the specified database connection.
     *
     * @param conn The database connection to use for executing SQL statements.
     */
    public static void createRepository(Properties Props, Connection conn) {
        ArrayList<Object> binds = new ArrayList<>();

        // Create Schema
        dbCommon.simpleUpdate(conn,String.format(REPO_DDL_SCHEMA,Props.getProperty("repo-schema"),Props.getProperty("repo-user")),binds, true);

        // Create Tables
        List<String> ddlConstants = List.of(
                REPO_DDL_DC_PROJECT,
                REPO_DDL_DC_RESULT,
                REPO_DDL_DC_SOURCE,
                REPO_DDL_DC_TABLE,
                REPO_DDL_DC_TABLE_COLUMN,
                REPO_DDL_DC_TABLE_COLUMN_MAP,
                REPO_DDL_DC_TABLE_HISTORY,
                REPO_DDL_DC_TABLE_MAP,
                REPO_DDL_DC_TARGET
        );

        ddlConstants.forEach(ddl -> dbCommon.simpleUpdate(conn, ddl, binds, true));

        // Create Indexes and Constraints
        List<String> ddlConstantsIndexConstraint = List.of(
                REPO_DDL_DC_RESULT_IDX1,
                REPO_DDL_DC_TABLE_HISTORY_IDX1,
                REPO_DDL_DC_TABLE_IDX1,
                REPO_DDL_DC_TABLE_COLUMN_IDX1,
                REPO_DDL_DC_TABLE_COLUMN_FK,
                REPO_DDL_DC_TABLE_MAP_FK,
                REPO_DDL_DC_TABLE_COLUMN_MAP_FK
        );

        ddlConstantsIndexConstraint.forEach(ddl -> dbCommon.simpleUpdate(conn, ddl, binds, true));

        // Data
        dbCommon.simpleUpdate(conn,REPO_DDL_DC_PROJECT_DATA, binds, true);

    }
}
