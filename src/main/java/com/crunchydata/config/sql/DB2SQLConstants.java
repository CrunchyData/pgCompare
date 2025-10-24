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

package com.crunchydata.config.sql;

public interface DB2SQLConstants {
    //
    // DB2 SQL
    //
    String SQL_DB2_SELECT_COLUMNS = """
            SELECT trim(c.TABSCHEMA) AS owner,
                   c.TABNAME AS table_name,
                   c.COLNAME AS column_name,
                   LOWER(c.TYPENAME) AS data_type,
                   c.LENGTH AS data_length,
                   COALESCE(c.length, 44) AS data_precision,
                   COALESCE(c.SCALE, 22) AS data_scale,
                   c.NULLS AS nullable,
                   CASE WHEN pkc.COLNAME IS NULL THEN 'N' ELSE 'Y' END AS pk
            FROM SYSCAT.COLUMNS c
                 LEFT JOIN (SELECT k.TABSCHEMA, k.TABNAME, k.COLNAME
                            FROM SYSCAT.KEYCOLUSE k
                                 JOIN SYSCAT.TABCONST tc ON (k.CONSTNAME = tc.CONSTNAME AND tc.TYPE = 'P')
                           ) pkc ON (c.TABSCHEMA = pkc.TABSCHEMA AND c.TABNAME = pkc.TABNAME AND c.COLNAME = pkc.COLNAME)
            WHERE LOWER(c.TABSCHEMA) = LOWER(?)
              AND LOWER(c.TABNAME) = LOWER(?)
            ORDER BY c.TABSCHEMA, c.TABNAME, c.COLNAME
           """;

    String SQL_DB2_SELECT_TABLES = """
                SELECT trim(TABSCHEMA) AS owner, TABNAME AS table_name
                FROM SYSCAT.TABLES
                WHERE LOWER(TABSCHEMA) = LOWER(?)
                ORDER BY TABSCHEMA, TABNAME
                """;

    String SQL_DB2_SELECT_TABLE = """
                SELECT trim(TABSCHEMA) AS owner, TABNAME AS table_name
                FROM SYSCAT.TABLES
                WHERE LOWER(TABSCHEMA) = LOWER(?)
                      AND LOWER(TABNAME) = LOWER(?)
                ORDER BY TABSCHEMA, TABNAME
                """;

    String SQL_DB2_SELECT_VERSION = "SELECT service_level AS version FROM SYSIBMADM.ENV_INST_INFO";

}
