/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.performance;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author shenzhw
 */
public class TestGlobalTableInsertPerf extends AbstractMultiTreadBatchTester {
    public boolean parseArgs(String[] args) {
        if (args.length < 5) {
            System.out
                    .println("input param,format: [jdbcurl] [user] [password]  [threadpoolsize]  recordcount ");
            return false;
        }
        url = args[0];
        user = args[1];
        password = args[2];
        threadCount = Integer.parseInt(args[3]);
        rangeItems = new String[]{"0-" + Integer.parseInt(args[4])};
        return true;

    }

    public static void main(String[] args) throws Exception {
        new TestGlobalTableInsertPerf().run(args);


    }

    @Override
    public Runnable createJob(SimpleConPool conPool2, long myCount,
                              int batch, long startId, AtomicLong finshiedCount2,
                              AtomicLong failedCount2) {
        return new GoodsInsertJob(conPool2,
                myCount, batch, startId, finshiedCount, failedCount);
    }


}