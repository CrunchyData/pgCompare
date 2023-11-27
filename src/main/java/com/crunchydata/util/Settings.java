package com.crunchydata.util;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class Settings {
    public static Properties Props;
    public static String version = "2023.11.09";

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

        return defaultProps;
    }


}
