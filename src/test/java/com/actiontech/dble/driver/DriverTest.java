package com.actiontech.dble.driver;
/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class DriverTest {

    @Test
    public void testTenant() throws Exception {
        Class.forName("com.mysql.jdbc.Driver");
        Connection connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:8066?connectionAttributes=tenant:tenant1", "root2", "123456");
        Statement stmt = connection.createStatement();
        stmt.executeQuery("select 1");
        ResultSet rs = stmt.getResultSet();
        Connection connection2 = DriverManager.getConnection("jdbc:mysql://127.0.0.1:8066", "root2:tenant1", "123456");
        Statement stmt2 = connection2.createStatement();
        stmt2.executeQuery("select 1");
        ResultSet rs2 = stmt2.getResultSet();
        Connection connection3 = DriverManager.getConnection("jdbc:mysql://127.0.0.1:8066", "root", "123456");
        Statement stmt3 = connection3.createStatement();
        stmt3.executeQuery("select 1");
        ResultSet rs3 = stmt3.getResultSet();
        rs.next();
        rs2.next();
        rs3.next();
        Assert.assertEquals(rs.getString(1), rs2.getString(1), rs3.getString(1));
        stmt.close();
        connection.close();
        stmt2.close();
        connection2.close();
        stmt3.close();
        connection3.close();
        Connection connection4 = DriverManager.getConnection("jdbc:mysql://127.0.0.1:9066", "man1", "654321");
        Statement stmt4 = connection4.createStatement();
        stmt4.executeQuery("show @@sql");
        ResultSet rs4 = stmt4.getResultSet();
        while (rs4.next()) {
            System.out.println(rs4.getString(2));
        }
        stmt4.close();
        connection4.close();
    }
}
