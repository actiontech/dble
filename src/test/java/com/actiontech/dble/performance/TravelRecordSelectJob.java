/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.performance;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class TravelRecordSelectJob implements Runnable, SelectJob {
    private final Connection con;
    private final long minId;
    private final long maxId;
    private final int executeTimes;
    Random random = new Random();
    private final AtomicInteger finshiedCount;
    private final AtomicInteger failedCount;
    private volatile long usedTime;
    private volatile long success;
    private volatile long maxTTL = 0;
    private volatile long minTTL = Integer.MAX_VALUE;
    private volatile long validTTLSum = 0;
    private volatile long validTTLCount = 0;

    public TravelRecordSelectJob(Connection con, long minId, long maxId,
                                 int executeTimes, AtomicInteger finshiedCount,
                                 AtomicInteger failedCount) {
        super();
        this.con = con;
        this.minId = minId;
        this.maxId = maxId;
        this.executeTimes = executeTimes;
        this.finshiedCount = finshiedCount;
        this.failedCount = failedCount;
    }

    private long select() {
        ResultSet rs = null;
        long used = -1;

        try {

            String sql = "select * from  travelrecord  where id="
                    + ((Math.abs(random.nextLong()) % (maxId - minId)) + minId);
            long startTime = System.currentTimeMillis();
            rs = con.createStatement().executeQuery(sql);
            if (rs.next()) {
            }
            used = System.currentTimeMillis() - startTime;
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
        return used;
    }

    @Override
    public void run() {
        long curmaxTTL = this.maxTTL;
        long curminTTL = this.minTTL;
        long curvalidTTLSum = this.validTTLSum;
        long curvalidTTLCount = this.validTTLCount;

        long start = System.currentTimeMillis();
        for (int i = 0; i < executeTimes; i++) {

            long ttlTime = this.select();
            if (ttlTime != -1) {
                if (ttlTime > curmaxTTL) {
                    curmaxTTL = ttlTime;
                } else if (ttlTime < curminTTL) {
                    curminTTL = ttlTime;
                }
                curvalidTTLSum += ttlTime;
                curvalidTTLCount += 1;
            }
            usedTime = System.currentTimeMillis() - start;
        }
        maxTTL = curmaxTTL;
        minTTL = curminTTL;
        validTTLSum = curvalidTTLSum;
        validTTLCount = curvalidTTLCount;

        try {
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public long getUsedTime() {
        return this.usedTime;
    }

    public double getTPS() {
        if (usedTime > 0) {
            return (this.success * 1000 + 0.0) / this.usedTime;
        } else {
            return 0;
        }
    }


    public long getMaxTTL() {
        return maxTTL;
    }

    public long getMinTTL() {
        return minTTL;
    }

    public long getValidTTLSum() {
        return validTTLSum;
    }

    public long getValidTTLCount() {
        return validTTLCount;
    }

    public static void main(String[] args) {
        Random r = new Random();
        for (int i = 0; i < 10; i++) {
            int f = r.nextInt(90000 - 80000) + 80000;
            System.out.println(f);
        }
    }
}