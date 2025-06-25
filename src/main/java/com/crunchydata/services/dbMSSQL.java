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

/**
 * Utility class for interacting with Microsoft SQL Server databases.
 *     MSSQL Data Types
 *         Date/Time: datetime, smalldatetime, date, time, datetimeoffset, datetime2
 *         Numeric: bigint, int, smallint, tinyint, decimal, numeric, money, smallmoney
 *         String: char, varchar, text, nchar, nvarchar, ntext, xml
 *         Unsupported: bit, binary, varbinary, image, cursor, rowversion, hierarchyid, uniqueidentifier, sql_variant
 *
 * @author Brian Pace
 */
public class dbMSSQL {
    public static final String nativeCase = "lower";
    public static final String quoteChar = "\"";
    public static final String columnHash= "lower(convert(varchar, hashbytes('MD5',%s),2)) AS %s";
}
