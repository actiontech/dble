/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/

package com.actiontech.dble.performance;

import java.sql.SQLException;

public class TestMaxConnection {
    public static void main(String[] args) {
        if (args.length < 4) {
            System.out
                    .println("input param,format: [jdbcurl] [user] [password]  [poolsize] ");
            return;
        }
        String url = args[0];
        String user = args[1];
        String password = args[2];
        Integer poolsize = Integer.parseInt(args[3]);
        SimpleConPool pool = null;
        long start = System.currentTimeMillis();
        try {
            pool = new SimpleConPool(url, user, password, poolsize);

        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("success create threadpool ,used time " + (System.currentTimeMillis() - start));
        int i = 0;
        try {
            for (i = 0; i < poolsize; i++) {
                pool.getConnection().createStatement()
                        .execute("select * from company limit 1");
            }
        } catch (SQLException e) {
            System.out.println("exectute  sql err " + i + " err:"
                    + e.toString());
        } finally {
            pool.close();
        }
    }
}
