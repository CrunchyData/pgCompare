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

package com.crunchydata.util;

public interface SQLConstantsOracle {
    //
    // Oracle SQL
    //
    String SQL_ORACLE_SELECT_COLUMNS = """
                SELECT c.owner, c.table_name, c.column_name, LOWER(c.data_type) data_type, c.data_length, nvl(c.data_precision,44) data_precision, nvl(c.data_scale,22) data_scale, c.nullable,
                       CASE WHEN pkc.column_name IS NULL THEN 'N' ELSE 'Y' END pk
                FROM all_tab_columns c
                     LEFT OUTER JOIN (SELECT con.owner, con.table_name, i.column_name, i.column_position
                                    FROM all_constraints con
                                         JOIN all_ind_columns i ON (con.index_owner=i.index_owner AND con.index_name=i.index_name)
                                    WHERE con.constraint_type='P') pkc ON (c.owner=pkc.owner AND c.table_name=pkc.table_name AND c.column_name=pkc.column_name)
                WHERE lower(c.owner)=lower(?)
                      AND lower(c.table_name)=lower(?)
                ORDER BY c.owner, c.table_name, c.column_name
                """;

    String SQL_ORACLE_SELECT_TABLES = """
                SELECT owner, table_name
                FROM all_tables
                WHERE lower(owner)=lower(?)
                ORDER BY owner, table_name
                """;

    String SQL_ORACLE_SELECT_VERSION = "select version from v$version";

}
