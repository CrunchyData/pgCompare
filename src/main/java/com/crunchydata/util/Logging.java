package com.crunchydata.util;

import java.util.Properties;
import java.util.logging.*;

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

    private static final Logger LOGGER = Logger.getLogger(Logging.class.getName());

    static {
        System.setProperty("java.util.logging.SimpleFormatter.format", "[%1$tF %1$tT] [%4$-7s] %5$s %n");
    }

    /**
     * Initializes the Logging class with the provided properties.
     *
     */
    public static void initialize() {

        // Set the log level based on the property value
        Level level = mapLogLevel(Props.getProperty("log-level", "INFO").toUpperCase());
        LOGGER.setLevel(level);

        LOGGER.setUseParentHandlers(false);

        Handler[] handlers = LOGGER.getHandlers();

        if (handlers.length == 0) {
            ConsoleHandler handler = new ConsoleHandler();
            handler.setLevel(level);
            handler.setFormatter(new SimpleFormatter());
            LOGGER.addHandler(handler);
        } else {
            for (Handler handler : handlers) {
                handler.setLevel(level);
            }
        }

        // Configure file handler if log-destination is not stdout
        String logDestination = Props.getProperty("log-destination", "stdout");
        if (!"stdout".equalsIgnoreCase(logDestination)) {
            try {
                FileHandler fileHandler = new FileHandler(logDestination);
                fileHandler.setFormatter(new SimpleFormatter());
                LOGGER.addHandler(fileHandler);
            } catch (Exception e) {
                System.out.println("Cannot allocate log file, will use stdout");
            }
        }
    }

    private static Level mapLogLevel(String setting) {
        return switch (setting.toUpperCase()) {
            case "DEBUG" -> Level.FINE;
            case "TRACE" -> Level.FINEST;
            case "INFO" -> Level.INFO;
            case "WARN", "WARNING" -> Level.WARNING;
            case "ERROR", "SEVERE" -> Level.SEVERE;
            case "ALL" -> Level.ALL;
            case "OFF" -> Level.OFF;
            default -> Level.INFO; // safe default
        };
    }

    /**
     * Writes a log message at the specified severity level.
     *
     * @param severity the severity level of the log message (info, warning, severe)
     * @param module   the module where the log message originated
     * @param message  the log message
     */
    public static void write(String severity, String module, String message) {
        String formattedMessage = String.format("[%-24s] %s", module, message);

        try {
            Level level = mapLogLevel(severity);
            LOGGER.log(level, formattedMessage);
        } catch (IllegalArgumentException e) {
            LOGGER.fine(formattedMessage);
        }

    }

}
