package com.crunchydata.util;

import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import static com.crunchydata.util.Settings.Props;

/**
 *
 * @author bpace
 */
public class Logging {

    final private static Logger logger;

    static {
        System.setProperty("java.util.logging.SimpleFormatter.format", "[%1$tF %1$tT] [%4$-7s] %5$s %n");

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
        if ( !Props.getProperty("log-destination").equals("stdout") ) {
            try {
                FileHandler fh = new FileHandler(Props.getProperty("log-destination"));
                SimpleFormatter formatter = new SimpleFormatter();
                fh.setFormatter(formatter);
                logger.addHandler(fh);
            } catch (Exception e) {
                System.out.println("Cannot allocate log file, will use stdout");
            }

        }
    }

    ////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////
    // write:
    //   Write log message at specified level.
    ////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////
    public static void write(String severity, String module, String message) {
        String msgFormat = "[%-20s] %2$s";

        switch (severity) {
            case "info":
                logger.info(String.format(msgFormat,module, message));
                break;
            case "warning":
                logger.warning(String.format(msgFormat,module, message));
                break;
            case "severe":
                logger.severe(String.format(msgFormat,module, message));
                break;
            default:
                logger.finer(String.format(msgFormat,module, message));
                break;
        }

    }

}

