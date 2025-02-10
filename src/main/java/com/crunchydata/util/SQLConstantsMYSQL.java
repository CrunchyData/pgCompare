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

public interface SQLConstantsMYSQL {
    //
    // MYSQL SQL
    //
    String SQL_MYSQL_SELECT_COLUMNS =  """
                SELECT c.table_schema owner, c.table_name, c.column_name, c.data_type,
                       coalesce(c.character_maximum_length,c.numeric_precision) data_length, coalesce(c.numeric_precision,44) data_precision, coalesce(c.numeric_scale,22) data_scale,
                       case when c.is_nullable='YES' then 'Y' else 'N' end nullable,
                       CASE WHEN pkc.column_name IS NULL THEN 'N' ELSE 'Y' END pk
                FROM information_schema.columns c
                     LEFT OUTER JOIN (SELECT tc.table_schema, tc.table_name, kcu.column_name, kcu.ORDINAL_POSITION column_position
                	  				  FROM information_schema.table_constraints tc
                					  	   INNER JOIN information_schema.key_column_usage kcu
                								ON tc.constraint_catalog = kcu.constraint_catalog
                									AND tc.constraint_schema = kcu.constraint_schema
                									AND tc.constraint_name = kcu.constraint_name
                									AND tc.table_name = kcu.table_name
                					WHERE tc.constraint_type='PRIMARY KEY')  pkc ON (c.table_schema=pkc.table_schema AND c.table_name=pkc.table_name AND c.column_name=pkc.column_name)
                WHERE lower(c.table_schema)=lower(?)
                      AND lower(c.table_name)=lower(?)
                ORDER BY c.table_schema, c.table_name, c.column_name
                """;

    String SQL_MYSQL_SELECT_TABLES = """
                SELECT table_schema owner, table_name table_name
                FROM  information_schema.tables
                WHERE lower(table_schema)=lower(?)
                      AND table_type = 'BASE TABLE'
                ORDER BY table_schema, table_name
                """;

    String SQL_MYSQL_SELECT_TABLE = """
                SELECT table_schema owner, table_name table_name
                FROM  information_schema.tables
                WHERE lower(table_schema)=lower(?)
                      AND table_type = 'BASE TABLE'
                      AND lower(table_name) = lower(?)
                ORDER BY table_schema, table_name
                """;

    String SQL_MYSQL_SELECT_VERSION = "select version()";

}
