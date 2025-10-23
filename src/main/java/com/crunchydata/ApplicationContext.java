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

package com.crunchydata;

import java.sql.Connection;
import java.sql.SQLException;

import static com.crunchydata.services.dbConnection.closeDatabaseConnection;
import static com.crunchydata.services.dbConnection.getConnection;
import static com.crunchydata.util.Settings.*;

import com.crunchydata.services.dbRepository;
import com.crunchydata.util.Logging;
import com.crunchydata.util.Preflight;
import com.crunchydata.util.Settings;

import org.apache.commons.cli.CommandLine;

/**
 * Application context class that manages the state and lifecycle of the pgCompare application.
 * This class encapsulates all application state and provides methods for initialization,
 * execution, and cleanup.
 * 
 * @author Brian Pace
 * @version 1.0
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
    
    // Application state
    private final CommandLine cmd;
    private final String action;
    private final Integer batchParameter;
    private final Integer pid;
    private final boolean genReport;
    private final String reportFileName;
    private final long startStopWatch;
    
    // Database connections
    private Connection connRepo;
    private Connection connSource;
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
        Props.setProperty("isCheck", Boolean.toString(ACTION_CHECK.equals(action)));

        // Initialize logging
        Logging.initialize();

        // Setup shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> 
            Logging.write("info", THREAD_NAME, "Shutting down")));

        // Log startup information
        logStartupInfo();
        
        // Connect to repository database
        connectToRepository();
        
        // Load project configuration (skip for init action)
        if (!ACTION_INIT.equals(action)) {
            setProjectConfig(connRepo, pid);
        }

        // Run preflight checks
        if (!Preflight.all(action)) {
            throw new RuntimeException("Preflight checks failed");
        }

        // Log configuration parameters
        logConfigurationParameters();
        
        // Handle initialization action
        if (ACTION_INIT.equals(action)) {
            handleInitialization();
        }
    }
    
    /**
     * Execute the main action based on the parsed command line arguments.
     * 
     * @throws Exception if action execution fails
     */
    public void executeAction() throws Exception {
        // Connect to source and target databases (skip for init action)
        if (!ACTION_INIT.equals(action)) {
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
     * Clean up resources and connections.
     */
    public void cleanup() {
        try {
            if (connRepo != null) {
                closeDatabaseConnection(connRepo);
            }
        } catch (Exception e) {
            Logging.write("warning", THREAD_NAME, "Error closing repository connection: " + e.getMessage());
        }
        
        try {
            if (connTarget != null) {
                closeDatabaseConnection(connTarget);
            }
        } catch (Exception e) {
            Logging.write("warning", THREAD_NAME, "Error closing target connection: " + e.getMessage());
        }
        
        try {
            if (connSource != null) {
                closeDatabaseConnection(connSource);
            }
        } catch (Exception e) {
            Logging.write("warning", THREAD_NAME, "Error closing source connection: " + e.getMessage());
        }
    }
    
    // Getters for application state
    public CommandLine getCmd() { return cmd; }
    public String getAction() { return action; }
    public Integer getBatchParameter() { return batchParameter; }
    public Integer getPid() { return pid; }
    public boolean isGenReport() { return genReport; }
    public String getReportFileName() { return reportFileName; }
    public long getStartStopWatch() { return startStopWatch; }
    public Connection getConnRepo() { return connRepo; }
    public Connection getConnSource() { return connSource; }
    public Connection getConnTarget() { return connTarget; }
    
    /**
     * Log startup information including run ID, version, and batch number.
     */
    private void logStartupInfo() {
        Logging.write("info", THREAD_NAME, String.format("Starting - rid: %s", startStopWatch));
        Logging.write("info", THREAD_NAME, String.format("Version: %s", Settings.VERSION));
        Logging.write("info", THREAD_NAME, String.format("Batch Number: %s", batchParameter));
    }
    
    /**
     * Connect to the repository database.
     * 
     * @throws Exception if connection fails
     */
    private void connectToRepository() throws Exception {
        Logging.write("info", THREAD_NAME, "Connecting to repository database");
        connRepo = getConnection(CONN_TYPE_POSTGRES, CONN_TYPE_REPO);
        if (connRepo == null) {
            throw new RuntimeException("Cannot connect to repository database");
        }
    }
    
    /**
     * Connect to source and target databases.
     * 
     * @throws Exception if connections fail
     */
    private void connectToSourceAndTarget() throws Exception {
        Logging.write("info", THREAD_NAME, "Connecting to source and target databases");
        connSource = getConnection(Props.getProperty("source-type"), CONN_TYPE_SOURCE);
        connTarget = getConnection(Props.getProperty("target-type"), CONN_TYPE_TARGET);
    }
    
    /**
     * Handle the initialization action.
     * 
     * @throws Exception if initialization fails
     */
    private void handleInitialization() throws Exception {
        Logging.write("info", THREAD_NAME, "Initializing pgCompare repository");
        dbRepository.createRepository(Props, connRepo);
        Logging.write("info", THREAD_NAME, "Repository initialization completed successfully");
        System.exit(0);
    }
    
    /**
     * Log configuration parameters (excluding passwords).
     */
    private void logConfigurationParameters() {
        Logging.write("info", THREAD_NAME, "Parameters: ");
        Props.entrySet().stream()
                .filter(e -> !e.getKey().toString().contains("password"))
                .sorted((e1, e2) -> e1.getKey().toString().compareTo(e2.getKey().toString()))
                .forEach(e -> Logging.write("info", THREAD_NAME, String.format("  %s", e)));
    }
    
    /**
     * Perform discovery operation.
     */
    private void performDiscovery() {
        Logging.write("info", THREAD_NAME, "Performing table discovery");
        String table = (cmd.hasOption("table")) ? cmd.getOptionValue("table").toLowerCase() : "";

        // Discover Tables
        com.crunchydata.controller.TableController.discoverTables(Props, pid, table, connRepo, connSource, connTarget);

        // Discover Columns
        com.crunchydata.controller.ColumnController.discoverColumns(Props, pid, table, connRepo, connSource, connTarget);
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
