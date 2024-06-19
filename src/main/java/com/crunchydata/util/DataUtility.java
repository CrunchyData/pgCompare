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
}
