/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sqlexecute;


import org.junit.Assert;

import java.io.*;
import java.sql.*;

/**
 * @author CrazyPig
 */
public class ServerPrepareTest {

    // JDBC driver name and database URL
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://localhost:8066/TESTDB?useServerPrepStmts=true";

    //  Database credentials
    static final String USER = "root";
    static final String PASS = "mysql";

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     *  create table hotnews (
     *  	id int primary key auto_increment,
     *  	title varchar(200),
     *  	content text,
     *  	image0 blob,
     *  	image1 blob,
     *  	image2 mediumblob,
     *  	image3 longblob
     *  ) engine = innodb default character set = 'utf8';
     */

    /**
     * TEST COM_STMT_SEND_LONG_DATA
     *
     * @throws IOException
     */
    public static void testComStmtSendLondData() throws IOException {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream image0In = classLoader.getResourceAsStream("blob/image0.jpg");
        InputStream image1In = classLoader.getResourceAsStream("blob/image1.png");
        InputStream image2In = classLoader.getResourceAsStream("blob/image2.png");
        InputStream image3In = classLoader.getResourceAsStream("blob/image3.png");

        byte[] image0Bytes = getBytes(image0In);
        byte[] image1Bytes = getBytes(image1In);
        byte[] image2Bytes = getBytes(image2In);
        byte[] image3Bytes = getBytes(image3In);

        try {
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            pstmt = conn.prepareStatement("insert into hotnews(id, title, content, image0, image1, image2, image3) values(?,?,?,?,?,?,?)");
            pstmt.setInt(1, 1314);
            pstmt.setString(2, "hotnew");
            pstmt.setBinaryStream(3, new ByteArrayInputStream("this is a content of hotnew".getBytes("UTF-8")));
            Blob image0Blob = conn.createBlob();
            Blob image1Blob = conn.createBlob();
            Blob image2Blob = conn.createBlob();
            Blob image3Blob = conn.createBlob();
            image0Blob.setBytes(1, image0Bytes);
            image1Blob.setBytes(1, image1Bytes);
            image2Blob.setBytes(1, image2Bytes);
            image3Blob.setBytes(1, image3Bytes);

            pstmt.setBlob(4, image0Blob);
            pstmt.setBlob(5, image1Blob);
            pstmt.setBlob(6, image2Blob);
            pstmt.setBlob(7, image3Blob);
            pstmt.execute();

            //  CHECK blob field which get from table
            pstmt = conn.prepareStatement("select image0, image1, image2, image3 from hotnews where id = ?");
            pstmt.setInt(1, 1314);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                InputStream _image0In = rs.getBlob(1).getBinaryStream();
                InputStream _image1In = rs.getBlob(2).getBinaryStream();
                InputStream _image2In = rs.getBlob(3).getBinaryStream();
                InputStream _image3In = rs.getBlob(4).getBinaryStream();

                Assert.assertArrayEquals(image0Bytes, getBytes(_image0In));
                Assert.assertArrayEquals(image1Bytes, getBytes(_image1In));
                Assert.assertArrayEquals(image2Bytes, getBytes(_image2In));
                Assert.assertArrayEquals(image3Bytes, getBytes(_image3In));
            }

            pstmt.close();

        } catch (SQLException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
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

    private static byte[] getBytes(InputStream in) throws IOException {
        byte[] bytes = new byte[0];
        byte[] buffer = new byte[1024];
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        int len = 0;
        while ((len = in.read(buffer)) > -1) {
            bout.write(buffer, 0, len);
        }
        bytes = bout.toByteArray();
        return bytes;
    }

    /**
     * TEST COM_STMT_RESET
     */
    public static void testComStmtRest() {
        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            pstmt = conn.prepareStatement("insert into hotnews(id, title, content) values(?,?,?)");
            pstmt.setInt(1, 1314);
            pstmt.setString(2, "hotnew");
            pstmt.setBinaryStream(3, new ByteArrayInputStream("this is a content of hotnew".getBytes("UTF-8")));
            pstmt.execute();
            pstmt.clearParameters();
            pstmt.setInt(1, 1315);
            pstmt.setString(2, "hotnew");
            pstmt.setBinaryStream(3, new ByteArrayInputStream("this is a new content of hotnew".getBytes("UTF-8")));
            pstmt.execute();
            pstmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
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

    public static void simpleTest() {
        Connection conn = null;
        PreparedStatement stmt = null;
        try {

            System.out.println("Connecting to database...");
            conn = DriverManager.getConnection(DB_URL, USER, PASS);

            System.out.println("Creating statement...");
            String sql = "SELECT *  FROM test  where id<?";

            stmt = conn.prepareStatement(sql);
            stmt.setInt(1, 8);
            ResultSet rs = stmt.executeQuery();
            // Extract data from result set
            ResultSetMetaData rsmd = rs.getMetaData();

            int colCount = rsmd.getColumnCount();
            for (int i = 1; i <= colCount; i++) {
                System.out.print(rsmd.getColumnName(i) + "\t");
            }
            System.out.println();
            while (rs.next()) {
                //Display values
                for (int i = 1; i <= colCount; i++) {
                    System.out.print(rs.getObject(i) + "\t");
                }
                System.out.println();
            }
            // Clean-up environment
            rs.close();
            stmt.close();
            conn.close();
        } catch (SQLException se) {
            //Handle errors for JDBC
            se.printStackTrace();
        } catch (Exception e) {
            //Handle errors for Class.forName
            e.printStackTrace();
        } finally {
            //finally block used to close resources
            try {
                if (stmt != null)
                    stmt.close();
            } catch (SQLException se2) {
            }// nothing we can do
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException se) {
                se.printStackTrace();
            }//end finally try
        }//end try
        System.out.println("Goodbye!");
    }

    public static void main(String[] args) {

        try {
            testComStmtSendLondData();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }//end main


    static void print(String name, ResultSet res)
            throws SQLException {
        System.out.println(name);
        ResultSetMetaData meta = res.getMetaData();
        StringBuilder str = new StringBuilder();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            str.append(meta.getColumnName(i)).append("   ");
        }
        System.out.println("\t" + str);
        str = new StringBuilder();
        while (res.next()) {
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                str.append(res.getString(i)).append("   ");
            }
            System.out.println("\t" + str);
            str = new StringBuilder();
        }
    }
}

