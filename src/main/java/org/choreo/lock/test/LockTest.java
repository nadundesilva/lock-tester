/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.choreo.lock.test;

import org.apache.commons.io.IOUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LockTest {
    private static final String MYSQL_DOCKER_CONTAINER_NAME = "mysql";
    private static final String MYSQL_SERVER_HOST = "localhost";
    private static final int MYSQL_SERVER_PORT = 30000;
    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PASSWORD = "root";
    private static final String MYSQL_DATABASE_NAME = "TestDB";
    private static final String JDBC_URL = "jdbc:mysql://" + MYSQL_SERVER_HOST + ":" + MYSQL_SERVER_PORT
            + "/" + MYSQL_DATABASE_NAME;
    private static final String MYSQL_TABLE_NAME = "TestTable";

    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException, SQLException {
        try {
            startMySQLServer();
            startTest();
        } finally {
            stopMySQLServer();
        }
    }

    private static void startTest() throws InterruptedException {
        print("Starting Test");
        System.out.println();

        RunnableThread workerTask = (String threadName) -> {
            try (Connection conn = DriverManager.getConnection(JDBC_URL, MYSQL_USERNAME, MYSQL_PASSWORD)) {
                conn.setAutoCommit(false);

                try (PreparedStatement stmt = conn.prepareStatement("START TRANSACTION")) {
                    stmt.execute();
                }

                int initialMaxNumber = -1;
                try (PreparedStatement stmt = conn.prepareStatement("SELECT MAX(ID) FROM " + MYSQL_TABLE_NAME
                        + " FOR UPDATE")) {
                    print("Trying to acquire write lock");
                    try (ResultSet resultSet = stmt.executeQuery()) {
                        print("Write lock acquired");
                        while (resultSet.next()) {
                            initialMaxNumber = resultSet.getInt(1);
                        }
                    }
                }
                if (initialMaxNumber > 0) {
                    print("Fetched initial max ID: " + initialMaxNumber);
                } else {
                    throw new RuntimeException("Failed to fetch initial max ID");
                }

                for (int i = 0; i < 20; i++) {
                    Thread.sleep(100);
                    try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO " + MYSQL_TABLE_NAME
                            + " (NAME) VALUES ('" + threadName + ":row-" + (i + 1) + "')")) {
                        int updatedRowCount = stmt.executeUpdate();
                        print("Iteration " + i + " - Inserted " + updatedRowCount + " row(s)");
                    }
                }

                try (PreparedStatement getStmt = conn.prepareStatement("SELECT MAX(ID) FROM " + MYSQL_TABLE_NAME)) {
                    try (ResultSet getStmtResultSet = getStmt.executeQuery()) {
                        while (getStmtResultSet.next()) {
                            int finalMaxNumber = getStmtResultSet.getInt(1);
                            print("Fetched final max ID: " + finalMaxNumber);
                            try (PreparedStatement stmt = conn.prepareStatement("SELECT NAME FROM " + MYSQL_TABLE_NAME
                                    + " WHERE ID="+ finalMaxNumber)) {
                                try (ResultSet resultSet = stmt.executeQuery()) {
                                    while (resultSet.next()) {
                                        print("Fetched final max ID name: " + resultSet.getString(1));
                                    }
                                }
                            }
                        }
                    }
                }

                print("Releasing write lock");
                conn.commit();
                conn.setAutoCommit(true);
            }
        };

        Thread worker1 = runThread("worker-1", workerTask);
        Thread worker2 = runThread("worker-2", workerTask);
        Thread worker3 = runThread("worker-3", workerTask);
        worker1.join();
        worker2.join();
        worker3.join();

        System.out.println();
        print("Test Complete");
    }

    private static void startMySQLServer() throws IOException, InterruptedException, ClassNotFoundException {
        File initScriptFile = new File("./temp/init.sql");
        initScriptFile.getParentFile().mkdirs();
        try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("init.sql")) {
            String initScript = IOUtils.toString(in, StandardCharsets.UTF_8);
            initScript = initScript.replaceAll("\\$\\{MYSQL_DATABASE_NAME}", MYSQL_DATABASE_NAME);
            initScript = initScript.replaceAll("\\$\\{MYSQL_TABLE_NAME}", MYSQL_TABLE_NAME);
            try (FileOutputStream out = new FileOutputStream(initScriptFile)) {
                IOUtils.write(initScript, out, StandardCharsets.UTF_8);
                out.flush();
            }
        }
        initScriptFile.deleteOnExit();

        print("Starting MySQL Server");
        executeDockerCommand("run", "--name", MYSQL_DOCKER_CONTAINER_NAME, "-d",
                "-p", MYSQL_SERVER_PORT + ":3306",
                "-v", initScriptFile.getParentFile().getCanonicalPath() + ":/docker-entrypoint-initdb.d",
                "-e", "MYSQL_ROOT_PASSWORD=" + MYSQL_PASSWORD,
                "-e", "MYSQL_DATABASE=" + MYSQL_DATABASE_NAME,
                "mysql:5.7");
        Class.forName("org.gjt.mm.mysql.Driver");
        while (true) {
            print("Trying to Connect to MySQL Server");
            try (Connection conn = DriverManager.getConnection(JDBC_URL, MYSQL_USERNAME, MYSQL_PASSWORD)) {
                conn.setAutoCommit(true);
                try (PreparedStatement stmt = conn.prepareStatement("SELECT * FROM " + MYSQL_TABLE_NAME
                        + " LIMIT 1")) {
                    if (stmt.execute()) {
                        print("Started MySQL Server");
                        break;
                    } else {
                        Thread.sleep(1000);
                    }
                } catch (SQLException e) {
                    Thread.sleep(1000);
                }
            } catch (SQLException e) {
                Thread.sleep(1000);
            }
        }
    }

    private static void stopMySQLServer() throws IOException, InterruptedException {
        print("Stopping MySQL Server");
        executeDockerCommand("stop", MYSQL_DOCKER_CONTAINER_NAME);
        executeDockerCommand("rm", MYSQL_DOCKER_CONTAINER_NAME);
        print("Stopped MySQL Server");
    }

    private static void executeDockerCommand(String ...args) throws IOException, InterruptedException {
        List<String> argsList = new ArrayList<>(args.length + 1);
        argsList.add("docker");
        argsList.addAll(Arrays.asList(args));
        ProcessBuilder processBuilder = new ProcessBuilder(argsList.toArray(new String[0]));
        processBuilder.redirectErrorStream(true);
        Process process = processBuilder.start();

        try (InputStreamReader reader = new InputStreamReader(process.getInputStream())) {
            try (BufferedReader in = new BufferedReader(reader)) {
                String line;
                while ((line = in.readLine()) != null) {
                    System.out.println("[docker-container] " + line);
                }
            }
        }

        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new RuntimeException("Docker command failed; Exit Code " + exitCode);
        }
    }

    private static Thread runThread(String threadName, RunnableThread runnable) {
        Thread thread = new Thread(() -> {
            try {
                runnable.run(threadName);
            } catch (Throwable t) {
                print("Thread " + threadName + " failed due to " + t.getMessage());
            }
        }, threadName);
        thread.start();
        return thread;
    }

    @FunctionalInterface
    private interface RunnableThread {
        void run(String threadName) throws Throwable;
    }

    private static void print(String text) {
        System.out.println("[" + Thread.currentThread().getName() + "] " + text);
    }
}
