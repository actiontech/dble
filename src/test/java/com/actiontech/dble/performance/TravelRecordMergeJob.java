/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.performance;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class TravelRecordMergeJob implements Runnable {
    private final Connection con;
    private final int executeTimes;
    Random random = new Random();
    private final AtomicInteger finshiedCount;
    private final AtomicInteger failedCount;
    private volatile long usedTime;
    private volatile int success;

    public TravelRecordMergeJob(Connection con, int executeTimes,
                                AtomicInteger finshiedCount, AtomicInteger failedCount) {
        super();
        this.con = con;
        this.executeTimes = executeTimes;
        this.finshiedCount = finshiedCount;
        this.failedCount = failedCount;
    }

    private void select() {
        ResultSet rs = null;
        try {
            String sql = "select sum(fee) total_fee, days,count(id),max(fee),min(fee) from  travelrecord   where days = "
                    + (Math.abs(random.nextInt()) % 10000)
                    + " or days ="
                    + (Math.abs(random.nextInt()) % 10000) + "  group by days  order by days desc";
            rs = con.createStatement().executeQuery(sql);
            finshiedCount.addAndGet(1);
            success++;
        } catch (Exception e) {
            failedCount.addAndGet(1);
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }

            }
        }
    }

    @Override
    public void run() {
        long start = System.currentTimeMillis();
        for (int i = 0; i < executeTimes; i++) {
            this.select();
            usedTime = System.currentTimeMillis() - start;
        }
        try {
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public long getUsedTime() {
        return this.usedTime;
    }

    public int getTPS() {
        if (usedTime > 0) {
            return (int) (this.success * 1000 / this.usedTime);
        } else {
            return 0;
        }
    }
}