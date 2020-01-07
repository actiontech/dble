/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.statistic.stat;

import com.actiontech.dble.server.parser.ServerParse;

import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicLong;

/**
 * UserSqlRWStat
 *
 * @author zhuam
 */
public class UserSqlRWStat {


    /**
     * R/W count
     */
    private AtomicLong rCount = new AtomicLong(0L);
    private AtomicLong wCount = new AtomicLong(0L);

    /**
     * QPS
     */
    private int qps = 0;

    /**
     * Net In/Out
     */
    private AtomicLong netInBytes = new AtomicLong(0L);
    private AtomicLong netOutBytes = new AtomicLong(0L);

    /**
     * concurrentMax
     */
    private int concurrentMax = 1;

    /**
     * execute time
     * <p>
     * 10milliseconds,between 10 and 200milliseconds, in 1 second,>1s
     */
    private final Histogram timeHistogram = new Histogram(10, 200, 1000, 2000);

    /**
     * period
     * <p>
     * 22pm-06am,06am-13pm,13-18pm, 18-22pm
     */
    private final Histogram executeHistogram = new Histogram(6, 13, 18, 22);

    /**
     * no lock for performance
     */
    private long lastExecuteTime;


    private int timeZoneOffset = 0;

    public UserSqlRWStat() {
        this.timeZoneOffset = TimeZone.getDefault().getRawOffset();
    }

    public void reset() {
        this.rCount = new AtomicLong(0L);
        this.wCount = new AtomicLong(0L);
        this.concurrentMax = 1;
        this.lastExecuteTime = 0;
        this.netInBytes = new AtomicLong(0L);
        this.netOutBytes = new AtomicLong(0L);

        this.timeHistogram.reset();
        this.executeHistogram.reset();
    }

    public void add(int sqlType, String sql, long executeTime, long netIn, long netOut, long startTime, long endTime) {
        switch (sqlType) {
            case ServerParse.SELECT:
            case ServerParse.SHOW:
                this.rCount.incrementAndGet();
                break;
            case ServerParse.UPDATE:
            case ServerParse.INSERT:
            case ServerParse.DELETE:
            case ServerParse.REPLACE:
                this.wCount.incrementAndGet();
                break;
            default:
                break;
        }

        //SQL execute time
        if (executeTime <= 10) {
            this.timeHistogram.record(10);
        } else if (executeTime <= 200) {
            this.timeHistogram.record(200);
        } else if (executeTime <= 1000) {
            this.timeHistogram.record(1000);
        } else {
            this.timeHistogram.record(2000);
        }

        //SQL period
        int oneHour = 3600 * 1000;
        long hour0 = endTime / (24L * (long) oneHour) * (24L * (long) oneHour) - (long) timeZoneOffset;
        long hour06 = hour0 + 6L * (long) oneHour - 1L;
        long hour13 = hour0 + 13L * (long) oneHour - 1L;
        long hour18 = hour0 + 18L * (long) oneHour - 1L;
        long hour22 = hour0 + 22L * (long) oneHour - 1L;

        if (endTime <= hour06 || endTime > hour22) {
            this.executeHistogram.record(6);
        } else if (endTime <= hour13) {
            this.executeHistogram.record(13);
        } else if (endTime <= hour18) {
            this.executeHistogram.record(18);
        } else {
            this.executeHistogram.record(22);
        }

        this.lastExecuteTime = endTime;

        this.netInBytes.addAndGet(netIn);
        this.netOutBytes.addAndGet(netOut);
    }

    public long getLastExecuteTime() {
        return lastExecuteTime;
    }

    public long getNetInBytes() {
        return netInBytes.get();
    }

    public long getNetOutBytes() {
        return netOutBytes.get();
    }

    public int getConcurrentMax() {
        return concurrentMax;
    }

    public void setConcurrentMax(int concurrentMax) {
        this.concurrentMax = concurrentMax;
    }

    public int getQps() {
        return qps;
    }

    public void setQps(int qps) {
        this.qps = qps;
    }

    public long getRCount() {
        return this.rCount.get();
    }

    public long getWCount() {
        return this.wCount.get();
    }

    public Histogram getTimeHistogram() {
        return this.timeHistogram;
    }

    public Histogram getExecuteHistogram() {
        return this.executeHistogram;
    }

}
