/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.performance;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.CopyOnWriteArrayList;

public class SimpleConPool {
    private final String url;
    private final String user;
    private final String password;
    private CopyOnWriteArrayList<Connection> cons = new CopyOnWriteArrayList<Connection>();

    public SimpleConPool(String url, String user, String password, int maxCon)
            throws SQLException {
        super();
        this.url = url;
        this.user = user;
        this.password = password;
        for (int i = 0; i < maxCon; i++) {
            cons.add(getCon());
        }
        System.out.println("success ful created connections ,total :" + maxCon);
    }

    public void close() {
        for (Connection con : this.cons) {
            try {
                if (con != null && !con.isClosed()) {
                    con.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        cons.clear();
    }

    private Connection getCon() throws SQLException {
        Connection theCon = DriverManager.getConnection(url, user, password);
        return theCon;
    }

    public void returnCon(Connection con) {
        try {
            if (con.isClosed()) {
                System.out.println("closed connection ,aband");
            } else {
                this.cons.add(con);

            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public Connection getConnection() throws SQLException {
        Connection con = null;
        if (cons.isEmpty()) {
            System.out.println("warn no connection in pool,create new one");
            con = getCon();
            return con;
        } else {
            con = cons.remove(0);
        }
        if (con.isClosed()) {
            System.out.println("warn connection closed ,create new one");
            con = getCon();

        }
        return con;
    }
}