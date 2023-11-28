/*
 * Copyright 2012-2023 the original author or authors.
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

import java.io.BufferedReader;
import java.io.Reader;

/**
 * @author Brian Pace
 */
public class DataUtility {

    public static String clobToString(javax.sql.rowset.serial.SerialClob data)
    {
        final StringBuilder sb = new StringBuilder();

        try
        {
            final Reader reader = data.getCharacterStream();
            final BufferedReader br     = new BufferedReader(reader);

            int b;
            while(-1 != (b = br.read()))
            {
                sb.append((char)b);
            }

            br.close();
        }
        catch (Exception e)
        {
            return e.toString();
        }

        return sb.toString();
    }
}
