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

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author Brian Pace
 */
public class Settings {
    public static Properties Props;
    public static String version = "0.1.0";

    static {
        Properties configProperties = setDefaults();
        try {
            InputStream stream = new FileInputStream("confero.properties");

            configProperties.load(stream);
            stream.close();

        } catch (Exception e) {
            System.out.println("Error reading config file " + e);
            System.exit(1);
        }
        Props = configProperties;
    }

    public static Properties setDefaults() {
        Properties defaultProps = new Properties();

        defaultProps.setProperty("batch-fetch-size","2000");
        defaultProps.setProperty("batch-commit-size","2000");
        defaultProps.setProperty("batch-load-size","500000");
        defaultProps.setProperty("observer-throttle","true");
        defaultProps.setProperty("observer-throttle-size","1000000");
        defaultProps.setProperty("observer-vacuum","true");
        defaultProps.setProperty("same-rdbms-optimization","true");
        defaultProps.setProperty("log-destination","stdout");
        defaultProps.setProperty("log-level","INFO");

        return defaultProps;
    }


}
