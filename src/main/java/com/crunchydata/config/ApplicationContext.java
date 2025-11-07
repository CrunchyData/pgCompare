/*
 * Copyright 2012-2025 the original author or authors.
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

import java.sql.Connection;

import static com.crunchydata.service.DatabaseConnectionService.getConnection;
import static com.crunchydata.config.Settings.*;

import com.crunchydata.service.RepositoryInitializationService;
import com.crunchydata.util.LoggingUtils;
import com.crunchydata.util.ValidationUtils;

import lombok.Getter;
import org.apache.commons.cli.CommandLine;

/**
 * Application context class that manages the state and lifecycle of the pgCompare application.
 * This class encapsulates all application state and provides methods for initialization,
 * execution, and cleanup.
 * 
 * @author Brian Pace
 */
public class ApplicationContext {
    
    // Constants
    private static final String THREAD_NAME = "main";
    private static final String ACTION_CHECK = "check";
    private static final String ACTION_INIT = "init";
    private static final String CONN_TYPE_POSTGRES = "postgres";
    private static final String CONN_TYPE_REPO = "repo";
    private static final String CONN_TYPE_SOURCE = "source";
    private static final String CONN_TYPE_TARGET = "target";

    // Getters for application state
    @Getter
    private final CommandLine cmd;
    private final String action;
    @Getter
    private final Integer batchParameter;
    @Getter
    private final Integer pid;
    @Getter
    private final boolean genReport;
    @Getter
    private final String reportFileName;
    @Getter
    private final long startStopWatch;
    
    // Database connections
    @Getter
    private Connection connRepo;
    @Getter
    private Connection connSource;
    @Getter
    private Connection connTarget;
    
    /**
     * Constructor for ApplicationContext.
     * 
     * @param cmd Parsed command line arguments
     */
    public ApplicationContext(CommandLine cmd) {
        this.cmd = cmd;
        this.action = Props.getProperty("action", "compare");
        this.batchParameter = Integer.parseInt(Props.getProperty("batch", "0"));
        this.pid = Integer.parseInt(Props.getProperty("pid", "1"));
        this.genReport = Boolean.parseBoolean(Props.getProperty("genReport", "false"));
        this.reportFileName = Props.getProperty("reportFileName", "");
        this.startStopWatch = System.currentTimeMillis();
    }
    
    /**
     * Initialize the application context.
     * 
     * @throws Exception if initialization fails
     */
    public void initialize() throws Exception {
        // Set check property based on action
        Props.setProperty("isCheck", Boolean.toString(action.equals(ACTION_CHECK)));

        // Initialize logging
        LoggingUtils.initialize();

        // Setup shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> 
            LoggingUtils.write("info", THREAD_NAME, "Shutting down")));

        // Log startup information
        logStartupInfo();
        
        // Connect to repository database
        connectToRepository();
        
        // Load project configuration (skip for init action)
        if (!action.equals(ACTION_INIT)) {
            setProjectConfig(connRepo, pid);
        }

        // Run preflight checks
        if (!ValidationUtils.all(action)) {
            throw new RuntimeException("Preflight checks failed");
        }

        // Log configuration parameters
        logConfigurationParameters();
        
        // Handle initialization action
        if (action.equals(ACTION_INIT)) {
            handleRepoInitialization();
        }
    }
    
    /**
     * Execute the main action based on the parsed command line arguments.
     * 
     */
    public void executeAction() {
        // Connect to source and target databases (skip for init action)
        if (!action.equals(ACTION_INIT)) {
            connectToSourceAndTarget();
        }

        // Execute the requested action
        switch (action) {
            case "discover":
                performDiscovery();
                break;
            case "check":
            case "compare":
                performCompare();
                break;
            case "copy-table":
                performCopyTable();
                break;
            default:
                throw new IllegalArgumentException("Invalid action specified: " + action);
        }
    }

    /**
     * Log startup information including run ID, version, and batch number.
     */
    private void logStartupInfo() {
        LoggingUtils.write("info", THREAD_NAME, String.format("Starting - rid: %s", startStopWatch));
        LoggingUtils.write("info", THREAD_NAME, String.format("Version: %s", Settings.VERSION));
        LoggingUtils.write("info", THREAD_NAME, String.format("Batch Number: %s", batchParameter));
    }
    
    /**
     * Connect to the repository database.
     * 
     */
    private void connectToRepository() {
        LoggingUtils.write("info", THREAD_NAME, "Connecting to repository database");
        connRepo = getConnection(CONN_TYPE_POSTGRES, CONN_TYPE_REPO);
        if (connRepo == null) {
            throw new RuntimeException("Cannot connect to repository database");
        }
    }
    
    /**
     * Connect to source and target databases.
     * 
     */
    private void connectToSourceAndTarget() {
        LoggingUtils.write("info", THREAD_NAME, "Connecting to source and target databases");
        connSource = getConnection(Props.getProperty("source-type"), CONN_TYPE_SOURCE);
        connTarget = getConnection(Props.getProperty("target-type"), CONN_TYPE_TARGET);
    }

    /**
     * Handle the initialization action.
     *
     * @throws Exception if initialization fails
     */
    private void handleRepoInitialization() throws Exception {
        LoggingUtils.write("info", THREAD_NAME, "Initializing pgCompare repository");
        RepositoryInitializationService.createRepository(Props, connRepo);
        LoggingUtils.write("info", THREAD_NAME, "Repository initialization completed successfully");
        System.exit(0);
    }
    
    /**
     * Log configuration parameters (excluding passwords).
     */
    private void logConfigurationParameters() {
        LoggingUtils.write("info", THREAD_NAME, "Parameters: ");
        Props.entrySet().stream()
                .filter(e -> !e.getKey().toString().contains("password"))
                .sorted((e1, e2) -> e1.getKey().toString().compareTo(e2.getKey().toString()))
                .forEach(e -> LoggingUtils.write("info", THREAD_NAME, String.format("  %s", e)));
    }
    
    /**
     * Perform discovery operation.
     */
    private void performDiscovery() {
        LoggingUtils.write("info", THREAD_NAME, "Performing table discovery");
        String table = (cmd.hasOption("table")) ? cmd.getOptionValue("table").toLowerCase() : "";

        // Discover Tables
        com.crunchydata.controller.DiscoverController.performTableDiscovery(Props, pid, table, connRepo, connSource, connTarget);

        // Discover Columns
        com.crunchydata.controller.DiscoverController.performColumnDiscovery(Props, pid, table, connRepo, connSource, connTarget);
    }
    
    /**
     * Perform comparison operation.
     */
    private void performCompare() {
        com.crunchydata.controller.CompareController.performCompare(this);
    }
    
    /**
     * Perform copy table operation.
     */
    private void performCopyTable() {
        com.crunchydata.controller.TableController.performCopyTable(this);
    }
}
