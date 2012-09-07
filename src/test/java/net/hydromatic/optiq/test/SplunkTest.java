/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package net.hydromatic.optiq.test;

import junit.framework.TestCase;

import java.io.PrintStream;
import java.sql.*;
import java.util.Properties;

/**
 * Unit test of the Optiq adapter for Splunk.
 *
 * @author jhyde
 */
public class SplunkTest extends TestCase {

    public static final String SPLUNK_URL = "https://marmite.local:8089";
    public static final String SPLUNK_USER = "admin";
    public static final String SPLUNK_PASSWORD = "splunk";

    private void loadDriverClass() {
        try {
            Class.forName("net.hydromatic.optiq.impl.splunk.SplunkDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("driver not found", e);
        }
    }


    private void close(Connection connection, Statement statement) {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                // ignore
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    /**
     * Tests the vanity driver.
     */
    public void testVanityDriver() throws SQLException {
        loadDriverClass();
        Properties info = new Properties();
        info.setProperty("url", SPLUNK_URL);
        Connection connection =
            DriverManager.getConnection("jdbc:splunk:", info);
        connection.close();
    }

    /**
     * Reads from a table.
     */
    public void testSelect() throws SQLException {
        checkSql(
            "select \"source\", \"sourcetype\"\n"
            + "from \"splunk\".\"splunk\"");
        checkSql(
            "select \"sourcetype\"\n"
            + "from \"splunk\".\"splunk\"");
    }

    public void testSelectDistinct() throws SQLException {
        checkSql(
            "select distinct \"sourcetype\"\n"
            + "from \"splunk\".\"splunk\"");
    }

    public void testSql() throws SQLException {
        checkSql(
            "select \"sourcetype\"\n"
            + "from \"splunk\".\"splunk\"");
    }

    private void checkSql(String sql) throws SQLException {
        loadDriverClass();
        Connection connection = null;
        Statement statement = null;
        try {
            Properties info = new Properties();
            info.put("url", SPLUNK_URL);
            info.put("user", SPLUNK_USER);
            info.put("password", SPLUNK_PASSWORD);
            connection = DriverManager.getConnection("jdbc:splunk:", info);
            statement = connection.createStatement();
            final ResultSet resultSet =
                statement.executeQuery(
                    sql);
            output(resultSet, System.out);
        } finally {
            close(connection, statement);
        }
    }

    private void output(
        ResultSet resultSet, PrintStream out) throws SQLException
    {
        final ResultSetMetaData metaData = resultSet.getMetaData();
        final int columnCount = metaData.getColumnCount();
        while (resultSet.next()) {
            for (int i = 1;; i++) {
                out.print(resultSet.getString(i));
                if (i < columnCount) {
                    out.print(", ");
                } else {
                    out.println();
                    break;
                }
            }
        }
    }
}

// End SplunkTest.java
