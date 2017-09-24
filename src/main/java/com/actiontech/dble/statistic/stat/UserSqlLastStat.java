/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.statistic.stat;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * UserSqlLastStat
 *
 * @author zhuam
 */
public class UserSqlLastStat {
    private static final int MAX_RECORDS = 1024;
    private SortedSet<SqlLast> sqls;

    public UserSqlLastStat(int count) {
        this.sqls = new ConcurrentSkipListSet<>();
    }

    public List<SqlLast> getSqls() {
        List<SqlLast> keyList = new ArrayList<>(sqls);
        return keyList;
    }

    public void add(String sql, long executeTime, long startTime, long endTime) {
        SqlLast sqlLast = new SqlLast(sql, executeTime, startTime, endTime);
        sqls.add(sqlLast);
    }

    public void reset() {
        this.clear();
    }

    public void clear() {
        sqls.clear();
    }

    public void recycle() {
        if (sqls.size() > MAX_RECORDS) {
            SortedSet<SqlLast> sqls2 = new ConcurrentSkipListSet<>();
            List<SqlLast> keyList = new ArrayList<>(sqls);
            int i = 0;
            for (SqlLast key : keyList) {
                if (i == MAX_RECORDS) {
                    break;
                }
                sqls2.add(key);
                i++;
            }
            sqls = sqls2;
        }
    }

    /**
     * SqlLast
     */
    public static class SqlLast implements Comparable<SqlLast> {

        private String sql;
        private long executeTime;
        private long startTime;
        private long endTime;

        public SqlLast(String sql, long executeTime, long startTime, long endTime) {
            super();
            this.sql = sql;
            this.executeTime = executeTime;
            this.startTime = startTime;
            this.endTime = endTime;
        }

        public String getSql() {
            return sql;
        }

        public long getStartTime() {
            return startTime;
        }

        public long getExecuteTime() {
            return executeTime;
        }

        public long getEndTime() {
            return endTime;
        }

        @Override
        public int compareTo(SqlLast o) {
            long st1 = o == null ? 0 : o.getStartTime();
            return (int) (st1 - this.getStartTime());
        }

        @Override
        public int hashCode() {
            return (int) getStartTime();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof SqlLast) {
                return this.compareTo((SqlLast) obj) == 0;
            } else {
                return super.equals(obj);
            }
        }
    }

}
