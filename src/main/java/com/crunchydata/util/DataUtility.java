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

import com.crunchydata.services.*;

import javax.sql.rowset.serial.SerialException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;

import static com.crunchydata.util.ColumnUtility.reservedWords;

/**
 * A utility class for working with CLOB data.
 *
 * @author Brian Pace
 */
public class DataUtility {

    /**
     * Converts a CLOB to a string.
     *
     * @param data the CLOB data
     * @return the CLOB data as a string
     */
    public static String convertClobToString(javax.sql.rowset.serial.SerialClob data)
    {
        try (Reader reader = data.getCharacterStream(); BufferedReader br = new BufferedReader(reader)) {
            StringBuilder sb = new StringBuilder();
            int b;
            while ((b = br.read()) != -1) {
                sb.append((char) b);
            }
            return sb.toString();
        } catch (IOException | SerialException e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean allLower(String str) {
        for (char c : str.toCharArray()) {
            if (! Character.isLowerCase(c) && Character.isAlphabetic(c) ) {
                return false;
            }
        }

        return true;
    }

    public static boolean allUpper(String str) {
        for (char c : str.toCharArray()) {
            if (! Character.isUpperCase(c) && Character.isAlphabetic(c) ) {
                return false;
            }
        }

        return true;
    }

    public static String getNativeCase(String databasePlatform) {
        return switch (databasePlatform) {
            case "oracle" -> dbOracle.nativeCase;
            case "mariadb" -> dbMariaDB.nativeCase;
            case "mysql" -> dbMySQL.nativeCase;
            case "mssql" -> dbMSSQL.nativeCase;
            case "db2" -> dbDB2.nativeCase;
            default -> dbPostgres.nativeCase;
        };
    }

    public static String getQuoteString(String databasePlatform) {
        return switch (databasePlatform) {
            case "oracle" -> dbOracle.quoteChar;
            case "mariadb" -> dbMariaDB.quoteChar;
            case "mysql" -> dbMySQL.quoteChar;
            case "mssql" -> dbMSSQL.quoteChar;
            case "db2" -> dbDB2.quoteChar;
            default -> dbPostgres.quoteChar;
        };
    }

    public static boolean isMixedCase(String str) {
        boolean hasUpper = false;
        boolean hasLower = false;

        for (char c : str.toCharArray()) {
            if (Character.isUpperCase(c)) {
                hasUpper = true;
            } else if (Character.isLowerCase(c)) {
                hasLower = true;
            }

            // If both are true, it's mixed case
            if (hasUpper && hasLower) {
                return true;
            }
        }

        // Not mixed case if only one or neither is true
        return false;
    }

    public static boolean containsSpecialCharacter(String str) {
        return str.matches(".*[^a-zA-Z0-9_].*");
    }

    public static boolean preserveCase(String expectedCase, String str) {
        return (((expectedCase.equals("lower") ) ? ! allLower(str) : ! allUpper(str)) || Arrays.asList(reservedWords).equals(str) || containsSpecialCharacter(str));
    }

    public static String ShouldQuoteString(Boolean preserveCase, String str, String quoteChar) {
        return (preserveCase) ? String.format("%s%s%s", quoteChar, str, quoteChar) :  str;
    }
}
