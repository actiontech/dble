/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sqlexecute;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class RollbackTest {
    private static Connection getCon(String url, String user, String passwd)
            throws SQLException {
        Connection theCon = DriverManager.getConnection(url, user, passwd);
        return theCon;
    }

    public static void main(String[] args) {


    }

}
