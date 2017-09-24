/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.statistic.stat;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.statistic.SQLRecord;
import com.actiontech.dble.statistic.SQLRecorder;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * UserStat
 *
 * @author Ben
 */
public class UserStat {

    private long sqlSlowTime = 100;

    private String user;

    /**
     * concurrentMax
     */
    private final AtomicInteger runningCount = new AtomicInteger();
    private final AtomicInteger concurrentMax = new AtomicInteger();

    /**
     * sqlLargeStat
     */
    private UserSqlLargeStat sqlLargeStat = null;

    /**
     * sqlLastStat
     */
    private UserSqlLastStat sqlLastStat = null;

    /**
     * CURD
     */
    private UserSqlRWStat sqlRwStat = null;

    /**
     * sqlHighStat
     */
    private UserSqlHighStat sqlHighStat = null;

    /**
     * slow TOP 10
     */
    private SQLRecorder sqlRecorder;

    /**
     * big result
     */
    private SqlResultSizeRecorder sqlResultSizeRecorder = null;

    public UserStat(String user) {
        super();

        this.user = user;
        this.sqlRwStat = new UserSqlRWStat();
        this.sqlLastStat = new UserSqlLastStat(50);
        this.sqlLargeStat = new UserSqlLargeStat(10);
        this.sqlHighStat = new UserSqlHighStat();

        int size = DbleServer.getInstance().getConfig().getSystem().getSqlRecordCount();
        this.sqlRecorder = new SQLRecorder(size);
        this.sqlResultSizeRecorder = new SqlResultSizeRecorder();
    }

    public String getUser() {
        return user;
    }

    public SQLRecorder getSqlRecorder() {
        return sqlRecorder;
    }

    public UserSqlRWStat getRWStat() {
        return sqlRwStat;
    }

    public UserSqlLastStat getSqlLastStat() {
        return this.sqlLastStat;
    }

    public UserSqlLargeStat getSqlLargeRowStat() {
        return this.sqlLargeStat;
    }

    public UserSqlHighStat getSqlHigh() {
        return this.sqlHighStat;
    }

    public SqlResultSizeRecorder getSqlResultSizeRecorder() {
        return this.sqlResultSizeRecorder;
    }


    public void setSlowTime(long time) {
        this.sqlSlowTime = time;
        this.sqlRecorder.clear();
    }

    public void clearSql() {
        this.sqlLastStat.reset();
    }

    public void clearSqlslow() {
        this.sqlRecorder.clear();
    }

    public void clearRwStat() {
        this.sqlRwStat.reset();
    }

    public void reset() {
        this.sqlRecorder.clear();
        this.sqlResultSizeRecorder.clearSqlResultSet();
        this.sqlRwStat.reset();
        this.sqlLastStat.reset();
        this.sqlLargeStat.reset();
        this.sqlHighStat.clearSqlFrequency();
        this.runningCount.set(0);
        this.concurrentMax.set(0);
    }

    /**
     * @param sqlType
     * @param sql
     * @param startTime
     */
    public void update(int sqlType, String sql, long sqlRows,
                       long netInBytes, long netOutBytes, long startTime, long endTime, int rseultSetSize) {

        //-----------------------------------------------------
        int invoking = runningCount.incrementAndGet();
        for (; ; ) {
            int max = concurrentMax.get();
            if (invoking > max) {
                if (concurrentMax.compareAndSet(max, invoking)) {
                    break;
                }
            } else {
                break;
            }
        }
        //-----------------------------------------------------


        //slow sql
        long executeTime = endTime - startTime;
        if (executeTime >= sqlSlowTime) {
            SQLRecord record = new SQLRecord();
            record.setExecuteTime(executeTime);
            record.setStatement(sql);
            record.setStartTime(startTime);
            this.sqlRecorder.add(record);
        }

        //sqlRwStat
        this.sqlRwStat.setConcurrentMax(concurrentMax.get());
        this.sqlRwStat.add(sqlType, sql, executeTime, netInBytes, netOutBytes, startTime, endTime);

        //sqlLastStatSQL
        this.sqlLastStat.add(sql, executeTime, startTime, endTime);

        //sqlHighStat
        this.sqlHighStat.addSql(sql, executeTime, startTime, endTime);

        //sqlLargeStat large than 10000 rows
        if (sqlType == ServerParse.SELECT && sqlRows > 10000) {
            this.sqlLargeStat.add(sql, sqlRows, executeTime, startTime, endTime);
        }

        //big size sql
        if (rseultSetSize >= DbleServer.getInstance().getConfig().getSystem().getMaxResultSet()) {
            this.sqlResultSizeRecorder.addSql(sql, rseultSetSize);
        }

        //after
        //-----------------------------------------------------
        runningCount.decrementAndGet();
    }
}
