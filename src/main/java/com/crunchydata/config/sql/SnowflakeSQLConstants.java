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

public interface SnowflakeSQLConstants {
    //
    // Snowflake SQL
    //
    String SQL_SNOWFLAKE_SELECT_COLUMNS = """
                SELECT DISTINCT c.table_schema as "owner", c.table_name as "table_name", c.column_name as "column_name",
                        c.data_type as "data_type", coalesce(c.character_maximum_length, c.numeric_precision) as "data_length",
                        coalesce(c.numeric_precision, 44) as "data_precision", coalesce(c.numeric_scale, 22) as "data_scale",
                        case when c.is_nullable = 'YES' then 'Y' else 'N' end as "nullable",
                        'N' as "pk"
                FROM information_schema.columns c
                WHERE lower(c.table_schema) = lower(?)
                      AND lower(c.table_name) = lower(?)
                ORDER BY c.table_schema, c.table_name
                """;

    String SQL_SNOWFLAKE_SELECT_TABLES = """
                SELECT table_schema as "owner", table_name as "table_name"
                FROM information_schema.tables
                WHERE lower(table_schema) = lower(?)
                      AND table_type = 'BASE TABLE'
                ORDER BY table_schema, table_name
                """;

    String SQL_SNOWFLAKE_SELECT_TABLE = """
                SELECT table_schema as "owner", table_name as "table_name"
                FROM information_schema.tables
                WHERE lower(table_schema) = lower(?)
                      AND table_type = 'BASE TABLE'
                      AND lower(table_name) = lower(?)
                ORDER BY table_schema, table_name
                """;

    String SQL_SNOWFLAKE_SELECT_VERSION = "SELECT CURRENT_VERSION()";

}
