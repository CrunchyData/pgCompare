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

package com.crunchydata.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Golden-master tests for the temporal primary-key SQL (issue #107). No database.
 *
 * <p>The key text and filter expressions were validated against PostgreSQL 16, SQL Server
 * 2022, MySQL 8.4, MariaDB 11.4, Oracle 21/23 and Db2 12.1: each row is found by its own key text,
 * under non-default session settings (DateStyle, SET LANGUAGE/DATEFORMAT, NLS), and the
 * filter keeps the primary-key index. Re-validate on real databases before changing them.</p>
 *
 * @author pgCompare
 */
class TemporalKeyUtilsTest {

    private static final String COL = "col";

    @Nested
    @DisplayName("Which key columns are temporal")
    class IsTemporalKey {

        @ParameterizedTest
        @CsvSource({"postgres,timestamp", "postgres,timestamptz", "postgres,date", "postgres,time",
                "mssql,datetime", "mssql,datetime2", "mssql,smalldatetime", "mssql,date", "mssql,datetimeoffset",
                "oracle,date", "oracle,timestamp(6)", "oracle,timestamp(6) with time zone",
                "mysql,datetime", "mysql,timestamp", "mariadb,datetime", "db2,timestamp", "snowflake,timestamp_ntz",
                "snowflake,TIMESTAMP_LTZ"})
        void temporal(String platform, String dataType) {
            assertTrue(TemporalKeyUtils.isTemporalKey(platform, dataType));
        }

        @Test
        @DisplayName("mssql rowversion (reported as 'timestamp') is binary, not temporal")
        void mssqlRowversion() {
            assertFalse(TemporalKeyUtils.isTemporalKey("mssql", "timestamp"));
        }

        @ParameterizedTest
        @CsvSource({"mysql,year", "postgres,varchar", "postgres,int4", "oracle,number"})
        void notTemporal(String platform, String dataType) {
            assertFalse(TemporalKeyUtils.isTemporalKey(platform, dataType));
        }
    }

    @Nested
    @DisplayName("Key text written into the pk JSON")
    class KeyText {

        @ParameterizedTest(name = "{0} {1}")
        @CsvSource(delimiter = '|', value = {
                "postgres  | timestamp                     | to_char(col,'YYYY-MM-DD HH24:MI:SS.US')",
                "postgres  | date                          | to_char(col,'YYYY-MM-DD HH24:MI:SS.US')",
                "postgres  | timestamptz                   | to_char(col at time zone 'UTC','YYYY-MM-DD HH24:MI:SS.US')",
                "postgres  | time                          | to_char(col,'HH24:MI:SS.US')",
                "mssql     | datetime                      | convert(varchar(27), col, 121)",
                "mssql     | datetime2                     | convert(varchar(27), col, 121)",
                "mssql     | smalldatetime                 | convert(varchar(27), col, 121)",
                "mssql     | date                          | convert(varchar(19), cast(col as datetime2(0)), 121)",
                "mssql     | datetimeoffset                | convert(varchar(27), cast(switchoffset(col, '+00:00') as datetime2(7)), 121)",
                "mssql     | time                          | convert(varchar(16), col, 121)",
                "oracle    | date                          | to_char(col,'YYYY-MM-DD HH24:MI:SS')",
                "oracle    | timestamp(6)                  | to_char(col,'YYYY-MM-DD HH24:MI:SS.FF9')",
                "oracle    | timestamp(6) with time zone   | to_char(col at time zone 'UTC','YYYY-MM-DD HH24:MI:SS.FF9')",
                "mysql     | datetime                      | date_format(convert_tz(col,@@session.time_zone,'UTC'),'%Y-%m-%d %H:%i:%s.%f')",
                "mysql     | timestamp                     | date_format(convert_tz(col,@@session.time_zone,'UTC'),'%Y-%m-%d %H:%i:%s.%f')",
                "mysql     | date                          | date_format(col,'%Y-%m-%d %H:%i:%s.%f')",
                "mysql     | time                          | time_format(col,'%H:%i:%s.%f')",
                "mariadb   | datetime                      | date_format(col,'%Y-%m-%d %H:%i:%s.%f')",
                "mariadb   | time                          | time_format(col,'%H:%i:%s.%f')",
                "db2       | timestamp                     | to_char(col,'YYYY-MM-DD HH24:MI:SS.FF12')",
                "db2       | date                          | to_char(col,'YYYY-MM-DD HH24:MI:SS')",
                "db2       | time                          | char(col, JIS)",
                "snowflake | timestamp_ntz                 | to_char(col,'YYYY-MM-DD HH24:MI:SS.FF9')",
                "snowflake | timestamp_ltz                 | to_char(convert_timezone('UTC', col),'YYYY-MM-DD HH24:MI:SS.FF9')",
                "snowflake | date                          | to_char(cast(col as timestamp_ntz),'YYYY-MM-DD HH24:MI:SS')",
                "snowflake | time                          | to_char(col,'HH24:MI:SS.FF9')"
        })
        void keyText(String platform, String dataType, String expected) {
            assertEquals(expected, TemporalKeyUtils.keyText(platform, dataType, COL));
        }
    }

    @Nested
    @DisplayName("Bind parameter parsed by the check filter")
    class KeyParameter {

        @ParameterizedTest(name = "{0} {1}")
        @CsvSource(delimiter = '|', value = {
                "postgres  | timestamp                     | cast(? as timestamp)",
                "postgres  | date                          | cast(? as date)",
                "postgres  | timestamptz                   | (cast(? as timestamp) at time zone 'UTC')",
                "postgres  | time                          | cast(? as time)",
                "mssql     | datetime                      | cast(convert(datetime2(7), left(?, 27), 121) as datetime)",
                "mssql     | datetime2                     | convert(datetime2(7), left(?, 27), 121)",
                "mssql     | smalldatetime                 | cast(convert(datetime2(7), left(?, 27), 121) as smalldatetime)",
                "mssql     | date                          | cast(convert(datetime2(7), left(?, 27), 121) as date)",
                "mssql     | datetimeoffset                | todatetimeoffset(convert(datetime2(7), left(?, 27), 121), '+00:00')",
                "mssql     | time                          | cast(left(?, 16) as time)",
                "oracle    | date                          | to_date(substr(?, 1, 19), 'YYYY-MM-DD HH24:MI:SS')",
                "oracle    | timestamp(6)                  | to_timestamp(substr(?, 1, 29), 'YYYY-MM-DD HH24:MI:SS.FF9')",
                "oracle    | timestamp(6) with time zone   | from_tz(to_timestamp(substr(?, 1, 29), 'YYYY-MM-DD HH24:MI:SS.FF9'), 'UTC')",
                "mysql     | datetime                      | convert_tz(cast(? as datetime(6)),'UTC',@@session.time_zone)",
                "mysql     | date                          | cast(? as date)",
                "mysql     | time                          | cast(? as time(6))",
                "mariadb   | datetime                      | cast(? as datetime(6))",
                "mariadb   | date                          | cast(? as date)",
                "db2       | timestamp                     | cast(cast(? as varchar(40)) as timestamp(12))",
                "db2       | time                          | time(substr(cast(? as varchar(40)), 1, 8))",
                "db2       | date                          | date(cast(? as varchar(40)))",
                "snowflake | timestamp_ntz                 | to_timestamp_ntz(substr(?, 1, 29))",
                "snowflake | date                          | to_date(substr(?, 1, 10))"
        })
        void keyParameter(String platform, String dataType, String expected) {
            assertEquals(expected, TemporalKeyUtils.keyParameter(platform, dataType));
        }

        @Test
        void snowflakeTimestampTz() {
            assertEquals("to_timestamp_tz(substr(?, 1, 29) || ' +00:00')", TemporalKeyUtils.keyParameter("snowflake", "timestamp_tz"));
        }
    }

    @Nested
    @DisplayName("Call sites")
    class CallSites {

        @Test
        @DisplayName("pk JSON uses the key text, not replace(), for a temporal key")
        void pkJsonTemporal() {
            assertEquals(
                "'\"dt\": \"' || to_char(dt,'YYYY-MM-DD HH24:MI:SS.US') || '\"' ",
                JsonProcessingUtils.buildJsonExpression("postgres", "dt", "timestamp", "char", "||", "replace(%s,'\"', '\\\"')"));
        }

        @Test
        @DisplayName("pk JSON of a non-temporal char key is unchanged")
        void pkJsonCharUnchanged() {
            assertEquals(
                "'\"code\": \"' || replace(code,'\"', '\\\"') || '\"' ",
                JsonProcessingUtils.buildJsonExpression("postgres", "code", "varchar", "char", "||", "replace(%s,'\"', '\\\"')"));
        }

        @Test
        @DisplayName("pk JSON of a numeric key is unchanged")
        void pkJsonNumericUnchanged() {
            assertEquals("'\"id\": ' + trim(cast(id as varchar))",
                JsonProcessingUtils.buildJsonExpression("mssql", "id", "int", "numeric", "+", "replace(%s, '\"', '\\\"')"));
        }

        @Test
        void checkFilterTemporal() {
            assertEquals(" AND dt = cast(convert(datetime2(7), left(?, 27), 121) as datetime)",
                ColumnMetadataUtils.buildColumnFilter("mssql", "datetime", "dt"));
        }

        @Test
        void checkFilterNonTemporalUnchanged() {
            assertEquals(" AND id = ?", ColumnMetadataUtils.buildColumnFilter("postgres", "int4", "id"));
        }
    }
}
