package com.sanketika.common.util;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.util.StatusPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;

/**
 * Utility class to configure Logback dynamically
 */
public class LogbackConfigurer {

    private LogbackConfigurer() {
        // Utility class, prevent instantiation
    }

    /**
     * Attempts to configure Logback from an external file.
     * Falls back to classpath configuration if external file is not found.
     * Uses console-only logging.
     */
    public static void configureLogback() {
        // First logging will be with default configuration
        System.out.println("Configuring Logback for console output...");

        File externalLogbackConfig = new File("/data/conf/logback.xml");
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();

        try {
            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(context);
            context.reset();

            if (externalLogbackConfig.exists() && externalLogbackConfig.canRead()) {
                System.out.println("Loading Logback configuration from external file: " +
                                 externalLogbackConfig.getAbsolutePath());
                configurator.doConfigure(externalLogbackConfig);
            } else {
                // Try to explicitly find the logback.xml in the classpath
                URL logbackConfigUrl = LogbackConfigurer.class.getResource("/logback.xml");
                if (logbackConfigUrl != null) {
                    System.out.println("Loading Logback configuration from classpath at: " + logbackConfigUrl);
                    configurator.doConfigure(logbackConfigUrl);
                } else {
                    System.out.println("WARNING: No logback.xml found in classpath. Basic console logging will be used.");
                    // The LoggerContext reset will use Logback's default configuration
                }
            }

            // Print Logback's internal status
            StatusPrinter.printInCaseOfErrorsOrWarnings(context);

            // Now we can use configured logger
            Logger logger = LoggerFactory.getLogger(LogbackConfigurer.class);
            logger.info("Logback configured successfully with console logging");

        } catch (JoranException je) {
            System.err.println("Error configuring Logback: " + je.getMessage());
            je.printStackTrace();
        }
    }
}
