package com.crunchydata.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.logging.*;

import static com.crunchydata.config.Settings.Props;

/**
 * Utility class for logging operations.
 * Provides methods to initialize logging configurations and write log messages at various severity levels.
 *
 * <p>This class is not instantiable.</p>
 *
 * @author Brian Pace
 */
public final class LoggingUtils {

    private static final Logger LOGGER = Logger.getLogger(LoggingUtils.class.getName());
    private static final String STDOUT = "stdout";
    private static final String LOG_FORMAT_PROPERTY = "java.util.logging.SimpleFormatter.format";
    private static final String DEFAULT_LOG_FORMAT = "[%1$tF %1$tT] [%4$-7s] %5$s %n";
    private static final String MODULE_FORMAT = "[%-24s] %s";

    static {
        // Set default format for log messages
        System.setProperty(LOG_FORMAT_PROPERTY, DEFAULT_LOG_FORMAT);
    }

    // Private constructor to prevent instantiation
    private LoggingUtils() {
        throw new UnsupportedOperationException("Utility class");
    }

    /**
     * Initializes the logging configuration based on provided properties.
     */
    public static void initialize() {
        Level level = mapLogLevel(Props.getProperty("log-level", "INFO"));
        LOGGER.setLevel(level);
        LOGGER.setUseParentHandlers(false);

        setupConsoleHandler(level);
        setupFileHandler(level);
    }

    private static void setupConsoleHandler(Level level) {
        if (LOGGER.getHandlers().length == 0) {
            ConsoleHandler consoleHandler = new ConsoleHandler();
            consoleHandler.setLevel(level);
            consoleHandler.setFormatter(new SimpleFormatter());
            LOGGER.addHandler(consoleHandler);
        } else {
            for (Handler handler : LOGGER.getHandlers()) {
                handler.setLevel(level);
            }
        }
    }

    private static void setupFileHandler(Level level) {
        String destination = Props.getProperty("log-destination", STDOUT).trim();

        if (!STDOUT.equalsIgnoreCase(destination)) {
            try {
                // Ensure parent directory exists
                Files.createDirectories(Paths.get(destination).getParent());

                FileHandler fileHandler = new FileHandler(destination, true);
                fileHandler.setLevel(level);
                fileHandler.setFormatter(new SimpleFormatter());
                LOGGER.addHandler(fileHandler);
            } catch (IOException e) {
                System.err.printf("Warning: Cannot write to log file '%s'. Falling back to stdout.%n", destination);
            }
        }
    }

    private static Level mapLogLevel(String setting) {
        return switch (setting.trim().toUpperCase()) {
            case "DEBUG" -> Level.FINE;
            case "TRACE" -> Level.FINEST;
            case "WARN", "WARNING" -> Level.WARNING;
            case "ERROR", "SEVERE" -> Level.SEVERE;
            case "ALL" -> Level.ALL;
            case "OFF" -> Level.OFF;
            case "INFO" -> Level.INFO;
            default -> Level.INFO; // fallback
        };
    }

    /**
     * Logs a message with the specified severity.
     *
     * @param severity the severity level (e.g., INFO, WARNING, ERROR)
     * @param module   the source module name
     * @param message  the message to log
     */
    public static void write(String severity, String module, String message) {
        Level level = mapLogLevel(severity);
        String formattedMessage = String.format(MODULE_FORMAT, module, message);
        LOGGER.log(level, formattedMessage);
    }
}
