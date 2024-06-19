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

import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import static com.crunchydata.util.Settings.Props;

/**
 * Utility class for logging operations.
 * Provides methods to initialize logging configurations and write log messages at various severity levels.
 *
 * <p>This class is not instantiable.</p>
 *
 * @author Brian Pace
 */
public class Logging {

    private static final Logger logger;

    static {
        System.setProperty("java.util.logging.SimpleFormatter.format", "[%1$tF %1$tT] [%4$-7s] %5$s %n");

        // Set the log level based on the property value
        switch (Props.getProperty("log-level")) {
            case "SEVERE":
                java.util.logging.Logger.getLogger(Logging.class.getName()).setLevel(Level.SEVERE);
                break;
            case "WARNING":
                java.util.logging.Logger.getLogger(Logging.class.getName()).setLevel(Level.WARNING);
                break;
            default:
                java.util.logging.Logger.getLogger(Logging.class.getName()).setLevel(Level.INFO);
                break;
        }

        logger = Logger.getLogger(Logging.class.getName());

        // Configure file handler if log-destination is not stdout
        if (!"stdout".equals(Props.getProperty("log-destination"))) {
            try {
                FileHandler fileHandler = new FileHandler(Props.getProperty("log-destination"));
                SimpleFormatter formatter = new SimpleFormatter();
                fileHandler.setFormatter(formatter);
                logger.addHandler(fileHandler);
            } catch (Exception e) {
                System.out.println("Cannot allocate log file, will use stdout");
            }
        }

    }

    /**
     * Writes a log message at the specified severity level.
     *
     * @param severity the severity level of the log message (info, warning, severe)
     * @param module the module where the log message originated
     * @param message the log message
     */
    public static void write(String severity, String module, String message) {
        String msgFormat = "[%-24s] %2$s";

        String formattedMessage = String.format(msgFormat, module, message);

        switch (severity.toLowerCase()) {
            case "info":
                logger.info(formattedMessage);
                break;
            case "warning":
                logger.warning(formattedMessage);
                break;
            case "severe":
                logger.severe(formattedMessage);
                break;
            default:
                logger.finer(formattedMessage);
                break;
        }

    }

}

