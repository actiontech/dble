/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sqlexecute;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class TestPrepareSql {

    private static String url = "jdbc:mysql://localhost:8066/test?useServerPrepStmts=true"; // useServerPrepStmts
    private static String user = "zhuam";
    private static String password = "zhuam";

    static {
        try {
            // load MySql drive
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        testServerPrepareInsertWithBingParam();
    }

    /**
     * testServerPrepareInsertWithBingParam
     */
    public static void testServerPrepareInsertWithBingParam() {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, user, password);
            conn.setAutoCommit(false);
            String sql = "insert into v1test(id,name1) values(?,?)";
            PreparedStatement pstmt = conn.prepareStatement(sql);
            int startId = 100;
            int batchSize = 10;
            int count = 0;
            while (count < batchSize) {
                pstmt.setInt(1, (int) (System.currentTimeMillis() / 1000L) + startId);
                pstmt.setString(2, "wowo" + startId);
                startId++;
                count++;
                pstmt.addBatch();
            }
            pstmt.executeBatch();
            conn.setAutoCommit(true);
            pstmt.close();
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
    }

}
