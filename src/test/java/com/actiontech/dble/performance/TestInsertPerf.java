/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.performance;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author wuzh
 */
public class TestInsertPerf extends AbstractMultiTreadBatchTester {

    public static void main(String[] args) throws Exception {
        new TestInsertPerf().run(args);


    }

    @Override
    public Runnable createJob(SimpleConPool conPool2, long myCount, int batch,
                              long startId, AtomicLong finshiedCount2,
                              AtomicLong failedCount2) {
        return new TravelRecordInsertJob(conPool2,
                myCount, batch, startId, finshiedCount, failedCount);
    }


}