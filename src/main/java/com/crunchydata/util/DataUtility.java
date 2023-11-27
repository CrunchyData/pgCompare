package com.crunchydata.util;

import java.io.BufferedReader;
import java.io.Reader;

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
