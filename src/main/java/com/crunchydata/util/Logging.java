package com.crunchydata.util;

import java.util.Properties;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * Utility class for logging operations.
 * Provides methods to initialize logging configurations and write log messages at various severity levels.
 *
 * <p>This class is not instantiable.</p>
 *
 * @author Brian Pace
 */
public class Logging {

    private static final Logger logger = Logger.getLogger(Logging.class.getName());

    static {
        System.setProperty("java.util.logging.SimpleFormatter.format", "[%1$tF %1$tT] [%4$-7s] %5$s %n");
    }

    /**
     * Initializes the Logging class with the provided properties.
     *
     * @param Props the properties to configure logging
     */
    public static void initialize(Properties Props) {

        // Set the log level based on the property value
        String logLevel = Props.getProperty("log-level", "INFO").toUpperCase();
        Level level = Level.parse(logLevel);
        logger.setLevel(level);

        // Configure file handler if log-destination is not stdout
        String logDestination = Props.getProperty("log-destination", "stdout");
        if (!"stdout".equalsIgnoreCase(logDestination)) {
            try {
                FileHandler fileHandler = new FileHandler(logDestination);
                fileHandler.setFormatter(new SimpleFormatter());
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
     * @param module   the module where the log message originated
     * @param message  the log message
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
