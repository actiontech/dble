/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sqlexecute;

import java.net.URL;
import java.net.URLClassLoader;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * test mysql jdbc
 * <p>
 * <p>
 * <p>
 * <p>
 * <b>Note:</b> </br>
 * 1. put the class into a new project  </br>
 * 2. mkdir lib in project/ and add mysql jdbc driver into the dir
 * 3. don't add mysql jdbc in classpath
 *
 * @author CrazyPig
 * @since 2016-11-13
 */
public class MulitJdbcVersionTest {

    private static final String JDBC_URL = "jdbc:mysql://localhost:8066/TESTDB";
    private static final String USER = "root";
    private static final String PASSWORD = "123456";
    private static final Map<String, String> jdbcVersionMap = new HashMap<String, String>();
    private static final Map<String, Driver> tmpDriverMap = new HashMap<String, Driver>();

    // load jdbd river
    private static void dynamicLoadJdbc(String mysqlJdbcFile) throws Exception {
        URL u = new URL("jar:file:lib/" + mysqlJdbcFile + "!/");
        String classname = jdbcVersionMap.get(mysqlJdbcFile);
        URLClassLoader ucl = new URLClassLoader(new URL[]{u});
        Driver d = (Driver) Class.forName(classname, true, ucl).newInstance();
        DriverShim driver = new DriverShim(d);
        DriverManager.registerDriver(driver);
        tmpDriverMap.put(mysqlJdbcFile, driver);
    }

    // deregisterDriver
    private static void dynamicUnLoadJdbc(String mysqlJdbcFile) throws SQLException {
        DriverManager.deregisterDriver(tmpDriverMap.get(mysqlJdbcFile));
    }

    // test
    private static void testOneVersion(String mysqlJdbcFile) {

        System.out.println("start test mysql jdbc version : " + mysqlJdbcFile);

        try {
            dynamicLoadJdbc(mysqlJdbcFile);
        } catch (Exception e1) {
            e1.printStackTrace();
        }

        Connection conn = null;
        try {
            conn = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select user()");
            System.out.println("select user() output : ");
            while (rs.next()) {
                System.out.println(rs.getObject(1));
            }
            rs = stmt.executeQuery("show tables");
            System.out.println("show tables output : ");
            while (rs.next()) {
                System.out.println(rs.getObject(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        try {
            dynamicUnLoadJdbc(mysqlJdbcFile);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        System.out.println("end !!!");
        System.out.println();
    }

    public static void main(String[] args) {

        // multi verion mysql jdbc test

        // NOTE:put jar into the lib and don't add to classpath
        jdbcVersionMap.put("mysql-connector-java-6.0.3.jar", "com.mysql.cj.jdbc.Driver");
        jdbcVersionMap.put("mysql-connector-java-5.1.6.jar", "com.mysql.jdbc.Driver");
        jdbcVersionMap.put("mysql-connector-java-5.1.31.jar", "com.mysql.jdbc.Driver");
        jdbcVersionMap.put("mysql-connector-java-5.1.35.jar", "com.mysql.jdbc.Driver");
        jdbcVersionMap.put("mysql-connector-java-5.1.39.jar", "com.mysql.jdbc.Driver");

        // more jdbc driver...

        for (String mysqlJdbcFile : jdbcVersionMap.keySet()) {
            testOneVersion(mysqlJdbcFile);
        }

    }

}

class DriverShim implements Driver {
    private Driver driver;

    DriverShim(Driver d) {
        this.driver = d;
    }

    public boolean acceptsURL(String u) throws SQLException {
        return this.driver.acceptsURL(u);
    }

    public Connection connect(String u, Properties p) throws SQLException {
        return this.driver.connect(u, p);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return this.driver.getPropertyInfo(url, info);
    }

    @Override
    public int getMajorVersion() {
        return this.driver.getMajorVersion();
    }

    @Override
    public int getMinorVersion() {
        return this.driver.getMinorVersion();
    }

    @Override
    public boolean jdbcCompliant() {
        return this.driver.jdbcCompliant();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return this.driver.getParentLogger();
    }
}
