/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.parser;

import com.actiontech.dble.route.parser.ManagerParse;

/**
 * @author mycat
 */
public class ManagerParserTestPerf {

    public void testPerformance() {
        for (int i = 0; i < 250000; i++) {
            ManagerParse.parse("show databases");
            ManagerParse.parse("set autocommit=1");
            ManagerParse.parse(" show  @@datasource ");
            ManagerParse.parse("select id,name,value from t");
        }
    }

    public void testPerformanceWhere() {
        for (int i = 0; i < 500000; i++) {
            ManagerParse.parse(" show  @@datasource where shardingnode = 1");
            ManagerParse.parse(" show  @@shardingnode where sharding = 1");
        }
    }

}