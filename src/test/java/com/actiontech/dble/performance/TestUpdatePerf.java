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
public class TestUpdatePerf extends AbstractMultiTreadBatchTester {
    private int repeats = 1;

    public TestUpdatePerf(int repearts) {
        this.repeats = repearts;
        if (repeats > 1) {
            this.outputMiddleInf = false;
        }
    }

    public static void main(String[] args) throws Exception {
        int repeats = 1;
        if (args.length > 5) {
            repeats = Integer.parseInt(args[5]);
        }
        for (int i = 0; i < repeats; i++) {
            new TestUpdatePerf(repeats).run(args);
        }

    }

    @Override
    public Runnable createJob(SimpleConPool conPool2, long myCount, int batch,
                              long startId, AtomicLong finshiedCount2,
                              AtomicLong failedCount2) {
        return new TravelRecordUpdateJob(conPool2, myCount, batch, startId,
                finshiedCount, failedCount);
    }

}