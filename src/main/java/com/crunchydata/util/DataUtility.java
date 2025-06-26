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

import javax.sql.rowset.serial.SerialException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import static com.crunchydata.util.ColumnUtility.RESERVED_WORDS;

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
        return (((expectedCase.equals("lower") ) ? ! allLower(str) : ! allUpper(str)) || RESERVED_WORDS.contains(str) || containsSpecialCharacter(str));
    }

    /**
     * Reads the current row in the ResultSet and returns a concatenated string of all column values.
     * Columns are separated by a delimiter (e.g., comma, tab, etc.).
     *
     * @param rs        the ResultSet
     * @param delimiter the delimiter to separate column values
     * @return a string representing all rows, one per line
     * @throws SQLException if an SQL error occurs
     */
    public static String resultSetRowToString(ResultSet rs, String delimiter) throws SQLException {
        StringBuilder rowString = new StringBuilder();

        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        for (int i = 1; i <= columnCount; i++) {
            Object value = rs.getObject(i);
            rowString.append(value != null ? value.toString() : "NULL");

            if (i < columnCount) {
                rowString.append(delimiter);
            }
        }

        return rowString.toString();
    }

    /**
     * Analyzes the passed string and based on the preserveCase will return a quoted string or the passed string.
     *
     * @param preserveCase      Should the case be preserved (quoted)
     * @param str               The string to be quoted or not quoted.
     * @return a string of the passed value either quoted or not quoted.
     */
    public static String ShouldQuoteString(Boolean preserveCase, String str, String quoteChar) {
        return (preserveCase) ? String.format("%s%s%s", quoteChar, str, quoteChar) :  str;
    }
}
