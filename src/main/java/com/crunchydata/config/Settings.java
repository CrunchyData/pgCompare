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

package com.crunchydata.config;

import com.crunchydata.controller.RepoController;
import com.crunchydata.util.FileSystemUtils;
import com.crunchydata.util.LoggingUtils;
import org.json.JSONObject;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Utility class for managing application settings.
 * Loads default settings, configuration from a properties file, and environment variables.
 *
 * <p>This class is not instantiable.</p>
 *
 * <p>The properties file location can be specified using the environment variable PGCOMPARE_CONFIG.</p>
 * <p>If the properties file is not found, default settings and environment variables will be used.</p>
 *
 * <p>The version of the settings is {@link #VERSION}.</p>
 *
 * <p>The loaded properties are accessible through {@link #Props}.</p>
 *
 * <p>The default settings are loaded using {@link #setDefaults()}.</p>
 * <p>Environment variables are applied using {@link #setEnvironment(Properties)}.</p>
 *
 * <p>Usage example:</p>
 * <pre>
 * {@code
 * Properties props = Settings.Props;
 * String logLevel = props.getProperty("log-level");
 * }
 * </pre>
 *
 * @see java.util.Properties
 * @see java.lang.System#getenv()
 * @see java.io.FileInputStream
 * @see java.io.InputStream
 * @see java.io.IOException
 * @see java.lang.Exception
 *
 * @author Brian Pace
 */
public class Settings {

    public static Properties Props;
    public static final String VERSION = "0.5.0.0";
    private static final String paramFile = (System.getenv("PGCOMPARE_CONFIG") == null) ? "pgcompare.properties" : System.getenv("PGCOMPARE_CONFIG");

    public static Map<String, Set<String>> validPropertyValues = Map.of(
            "column-hash-method", Set.of("database", "hybrid", "raw"),
            "number-cast", Set.of("notation", "standard"),
            "source-type", Set.of("db2", "oracle", "postgres", "mariadb", "mssql", "mysql", "snowflake"),
            "target-type", Set.of("db2", "oracle", "postgres", "mariadb", "mssql", "mysql", "snowflake")
    );

    static {
         Properties configProperties = setDefaults();

         if ( FileSystemUtils.FileExistsCheck(paramFile)) {
            try (InputStream stream = new FileInputStream(paramFile)) {
                configProperties.load(stream);
            } catch (Exception e) {
                LoggingUtils.write("warning","Settings", "Configuration file not found, using defaults, project, and environment variables");
            }
         }

        // Trim all values in the Properties object
        configProperties.forEach((key, value) -> {
            if (value instanceof String) {
                configProperties.setProperty((String) key, ((String) value).trim());
            }
        });

        Props = setEnvironment(configProperties);
    }

    /**
     * Sets the default properties for the application.
     *
     * @return a {@code Properties} object containing the default settings
     */
    public static Properties setDefaults() {
        Properties defaultProps = new Properties();

        // System Settings
        //defaultProps.setProperty("project", "1");
        defaultProps.setProperty("config-file", paramFile);
        defaultProps.setProperty("batch-fetch-size","2000");
        defaultProps.setProperty("batch-commit-size","2000");
        defaultProps.setProperty("batch-progress-report-size","1000000");
        defaultProps.setProperty("column-hash-method","database");
        defaultProps.setProperty("database-sort","true");
        defaultProps.setProperty("float-scale","3");
        defaultProps.setProperty("loader-threads","0");
        defaultProps.setProperty("log-destination","stdout");
        defaultProps.setProperty("log-level","INFO");
        defaultProps.setProperty("message-queue-size","1000");
        defaultProps.setProperty("number-cast","notation");
        defaultProps.setProperty("observer-throttle","true");
        defaultProps.setProperty("observer-throttle-size","2000000");
        defaultProps.setProperty("observer-vacuum","true");
        defaultProps.setProperty("stage-table-parallel","0");
        defaultProps.setProperty("standard-number-format","0000000000000000000000.0000000000000000000000");


        // Repository
        defaultProps.setProperty("repo-dbname","pgcompare");
        defaultProps.setProperty("repo-host","localhost");
        defaultProps.setProperty("repo-password","welcome1");
        defaultProps.setProperty("repo-port","5432");
        defaultProps.setProperty("repo-schema","pgcompare");
        defaultProps.setProperty("repo-sslmode","disable");
        defaultProps.setProperty("repo-user","pgcompare");

        // Source
        defaultProps.setProperty("source-dbname","postgres");
        defaultProps.setProperty("source-host","localhost");
        defaultProps.setProperty("source-password","welcome1");
        defaultProps.setProperty("source-port","5432");
        defaultProps.setProperty("source-schema","");
        defaultProps.setProperty("source-sslmode","disable");
        defaultProps.setProperty("source-type","postgres");
        defaultProps.setProperty("source-user","postgres");
        defaultProps.setProperty("source-schema",defaultProps.getProperty("source-user"));

        // Target
        defaultProps.setProperty("target-dbname","postgres");
        defaultProps.setProperty("target-host","localhost");
        defaultProps.setProperty("target-password","welcome1");
        defaultProps.setProperty("target-port","5432");
        defaultProps.setProperty("target-schema","");
        defaultProps.setProperty("target-sslmode","disable");
        defaultProps.setProperty("target-type","postgres");
        defaultProps.setProperty("target-user","postgres");
        defaultProps.setProperty("target-schema",defaultProps.getProperty("target-user"));

        return defaultProps;
    }

    /**
     * Applies environment variables to the given properties object.
     *
     * @param prop the {@code Properties} object to which environment variables are applied
     * @return the updated {@code Properties} object with environment variables applied
     */
    public static Properties setEnvironment (Properties prop) {

        System.getenv().forEach((k, v) -> {
            if (k.contains("PGCOMPARE_")) {
                prop.setProperty(k.replace("PGCOMPARE_","").replace("_","-").toLowerCase(),v);
            }
        });

        return prop;

    }

    /**
     * Applies properties that are stored in the dc_project table.
     *
     * @param conn Connection to the repository database.
     * @param pid  Project ID.
     */
    public static void setProjectConfig (Connection conn, Integer pid) {

        JSONObject projectConfig = new JSONObject(RepoController.getProjectConfig(conn, pid));

        if ( ! projectConfig.isEmpty() ) {

            Iterator<String> keys = projectConfig.keys();

            while (keys.hasNext()) {
                String key = keys.next();
                String value = projectConfig.get(key).toString();

                Props.setProperty(key, value);

            }
        }

    }


}
