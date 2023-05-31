/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.statistic.stat;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class UserSqlHighStat {

    private static final int CAPACITY_SIZE = 1024;

    private Map<String, SqlFrequency> sqlFrequencyMap = new ConcurrentHashMap<>();



    private StatSqlParser sqlParser = new StatSqlParser();

    public void addSql(String sql, long executeTime, long startTime, long endTime) {
        String newSql = this.sqlParser.mergeSql(sql);
        SqlFrequency frequency = this.sqlFrequencyMap.get(newSql);
        if (frequency == null) {
            frequency = new SqlFrequency();
            frequency.setSql(newSql);
            SqlFrequency tmp = sqlFrequencyMap.putIfAbsent(newSql, frequency);
            if (tmp != null) {
                frequency = tmp;
            }
        }
        frequency.setLastTime(endTime);
        frequency.incCount();
        //TODO setExecuteTime has thread safe problem
        frequency.setExecuteTime(executeTime);
    }


    /**
     * getSqlFrequency
     */
    public List<SqlFrequency> getSqlFrequency(boolean isClear) {
        List<SqlFrequency> list = new ArrayList<>(this.sqlFrequencyMap.values());
        if (isClear) {
            clearSqlFrequency();
        }
        return list;
    }


    public void clearSqlFrequency() {
        sqlFrequencyMap.clear();
    }

    public void recycle() {
        if (sqlFrequencyMap.size() > CAPACITY_SIZE) {
            Map<String, SqlFrequency> sqlFrequencyMap2 = new ConcurrentHashMap<>();
            SortedSet<SqlFrequency> sqlFrequencySortedSet = new TreeSet<>(this.sqlFrequencyMap.values());
            List<SqlFrequency> keyList = new ArrayList<>(sqlFrequencySortedSet);
            int i = 0;
            for (SqlFrequency key : keyList) {
                if (i == CAPACITY_SIZE) {
                    break;
                }
                sqlFrequencyMap2.put(key.getSql(), key);
                i++;
            }
            sqlFrequencyMap = sqlFrequencyMap2;
        }
    }


}
