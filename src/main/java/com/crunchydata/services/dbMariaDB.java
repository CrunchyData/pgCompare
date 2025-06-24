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

import com.crunchydata.util.Logging;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Properties;

/**
 * Utility class for interacting with MariaDB databases.
 *     MariaDB Data Types
 *         Date/Time: date, datetime, timestamp, time, year
 *         Numeric: integer, smallint, decimal, numeric, float, real, double, int, dec, fixed
 *         String: char, varchar, text, json
 *         Unsupported: bit, binary, varbinary, blob, enum, set
 *
 * @author Brian Pace
 */
public class dbMariaDB {
    public static final String nativeCase = "lower";
    public static final String quoteChar = "`";
    public static final String columnHash= "lower(md5(%s)) AS %s";

    private static final String THREAD_NAME = "db-mariadb";
}
