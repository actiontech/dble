package demo.test;

import com.mysql.cj.jdbc.MysqlPooledConnection;

import java.sql.*;

public class TestJdbc {
    public static void main(String args[]) throws ClassNotFoundException, SQLException {

        TestJdbc obj = new TestJdbc();
        try {
            obj.test2();
        }catch (Exception e){
            System.out.println(e);
        }

    }
    public void test0() throws ClassNotFoundException, SQLException {
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
    public void test1() throws ClassNotFoundException, SQLException {
        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection conn = DriverManager.getConnection("jdbc:mysql://10.186.62.102:3307/?connectionAttributes=key1:value1", "root", "123456");
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
    public void test2() {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection connectionPrepare = DriverManager.getConnection(
                    "jdbc:mysql://10.186.62.102:3306/?connectionAttributes=key1:value1", "root", "123456");
            MysqlPooledConnection pool = new MysqlPooledConnection((com.mysql.cj.jdbc.JdbcConnection) connectionPrepare);
            Connection connection = pool.getConnection();
            connection.setCatalog("db_1");
            Statement stmt = connection.createStatement();
            stmt.execute("select * from test");
            stmt.close();
            connection.close();
        } catch (Exception e) {
            System.out.println(e);
        }

    }
}
