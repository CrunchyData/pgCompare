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
    public static String version = "1.1.1";

    static String paramFile = (System.getenv("CONFERODC_CONFIG") == null) ? "confero.properties" : System.getenv("CONFERODC_CONFIG");

    static {
            Properties configProperties = setDefaults();
            try {
                InputStream stream = new FileInputStream(paramFile);

                configProperties.load(stream);
                stream.close();

            } catch (Exception e) {
                System.out.println("Configuration file not found, using defaults and environment variables");
            }

            Props = setEnvironment(configProperties);
    }

    public static Properties setDefaults() {
        Properties defaultProps = new Properties();

        // System Settings
        defaultProps.setProperty("batch-fetch-size","2000");
        defaultProps.setProperty("batch-commit-size","2000");
        defaultProps.setProperty("batch-progress-report-size","1000000");
        defaultProps.setProperty("database-sort","true");
        defaultProps.setProperty("log-destination","stdout");
        defaultProps.setProperty("log-level","INFO");
        defaultProps.setProperty("number-cast","notation");
        defaultProps.setProperty("observer-throttle","true");
        defaultProps.setProperty("observer-throttle-size","2000000");
        defaultProps.setProperty("observer-vacuum","true");
        defaultProps.setProperty("stage-table-parallel","0");


        // Repository
        defaultProps.setProperty("repo-dbname","confero");
        defaultProps.setProperty("repo-host","localhost");
        defaultProps.setProperty("repo-password","welcome1");
        defaultProps.setProperty("repo-port","5432");
        defaultProps.setProperty("repo-schema","confero");
        defaultProps.setProperty("repo-sslmode","disable");
        defaultProps.setProperty("repo-user","confero");

        // Source
        defaultProps.setProperty("source-database-hash","true");
        defaultProps.setProperty("source-dbname","postgres");
        defaultProps.setProperty("source-host","localhost");
        defaultProps.setProperty("source-name","source");
        defaultProps.setProperty("source-password","welcome1");
        defaultProps.setProperty("source-port","5432");
        defaultProps.setProperty("source-sslmode","disable");
        defaultProps.setProperty("source-type","postgres");
        defaultProps.setProperty("source-user","postgres");

        // Target
        defaultProps.setProperty("target-database-hash","true");
        defaultProps.setProperty("target-dbname","postgres");
        defaultProps.setProperty("target-host","localhost");
        defaultProps.setProperty("target-name","target");
        defaultProps.setProperty("target-password","welcome1");
        defaultProps.setProperty("target-port","5432");
        defaultProps.setProperty("target-sslmode","disable");
        defaultProps.setProperty("target-type","postgres");
        defaultProps.setProperty("target-user","postgres");

        return defaultProps;
    }

    public static Properties setEnvironment (Properties prop) {

        System.getenv().forEach((k, v) -> {
            if (k.contains("CONFERODC-")) {
                prop.setProperty(k.replace("CONFERODC-","").toLowerCase(),v);
            }
        });

        return prop;

    }


}
