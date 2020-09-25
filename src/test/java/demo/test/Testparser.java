package demo.test;

import java.sql.*;

public class Testparser {
    public static void main(String args[]) throws ClassNotFoundException, SQLException {

        Class.forName("com.mysql.jdbc.Driver");
        Connection conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:8066/test?useServerPrepStmts=true&&useAffectedRows=true", "rw", "123456");


        //        Connection conn = DriverManager.getConnection("jdbc:mysql://10.186.61.151:33306/test?useServerPrepStmts=true&&useAffectedRows=true", "root", "123456");

        //        Statement stmt = conn.createStatement();
        //        stmt.execute("/*#dble:db_type=master*/select * from testdb.tb_enum_sharding;");
        //        stmt.close();
        //        conn.close();

        PreparedStatement stmt = conn.prepareStatement("select * from test where id=?");
        stmt.setInt(1, 1);

        ResultSet rs = stmt.executeQuery();
        while (rs.next()) {
            System.out.println(rs.getInt(1));
        }
        rs = stmt.executeQuery();
        while (rs.next()) {
            System.out.println(rs.getInt(1));
        }

        rs.close();
        stmt.close();
        conn.close();

    }
}
