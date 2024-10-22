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

import static com.crunchydata.util.SQLConstantsRepo.*;
import static com.crunchydata.util.Settings.Props;

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
    public static void createRepository(Connection conn) {
        ArrayList<Object> binds = new ArrayList<>();

        // Create Schema
        dbCommon.simpleUpdate(conn,String.format(REPO_DDL_SCHEMA,Props.getProperty("repo-schema"),Props.getProperty("repo-user")),binds, true);

        // Create Tables
        dbCommon.simpleUpdate(conn,REPO_DDL_DCPROJECT, binds, true);
        dbCommon.simpleUpdate(conn,REPO_DDL_DCTABLE, binds, true);
        dbCommon.simpleUpdate(conn,REPO_DDL_DCTABLE_COLUMN, binds, true);
        dbCommon.simpleUpdate(conn,REPO_DDL_DCTABLE_COLUMN_MAP, binds, true);
        dbCommon.simpleUpdate(conn,REPO_DDL_DCTABLE_HISTORY, binds, true);
        dbCommon.simpleUpdate(conn,REPO_DDL_DCTABLE_MAP, binds, true);
        dbCommon.simpleUpdate(conn,REPO_DDL_DCRESULT, binds, true);
        dbCommon.simpleUpdate(conn,REPO_DDL_DCSOURCE, binds, true);
        dbCommon.simpleUpdate(conn,REPO_DDL_DCTARGET, binds, true);

        // Create Indexes
        dbCommon.simpleUpdate(conn,REPO_DDL_DCRESULT_IDX1, binds, true);
        dbCommon.simpleUpdate(conn,REPO_DDL_DCTABLE_HISTORY_IDX1, binds, true);

        // Add Constraints
        dbCommon.simpleUpdate(conn,REPO_DDL_DCPROJECT_PK, binds, true);
        dbCommon.simpleUpdate(conn,REPO_DDL_DCTABLE_PK, binds, true);
        dbCommon.simpleUpdate(conn,REPO_DDL_DCTABLE_COLUMN_PK, binds, true);
        dbCommon.simpleUpdate(conn,REPO_DDL_DCTABLE_COLUMN_MAP_PK, binds, true);
        dbCommon.simpleUpdate(conn,REPO_DDL_DCTABLE_MAP_PK, binds, true);
        dbCommon.simpleUpdate(conn,REPO_DDL_DCTABLE_COLUMN_FK, binds, true);
        dbCommon.simpleUpdate(conn,REPO_DDL_DCTABLE_MAP_FK, binds, true);
        dbCommon.simpleUpdate(conn,REPO_DDL_DCTABLE_COLUMN_MAP_FK, binds, true);

        // Data
        dbCommon.simpleUpdate(conn,REPO_DDL_DCPROJECT_DATA, binds, true);

    }
}
