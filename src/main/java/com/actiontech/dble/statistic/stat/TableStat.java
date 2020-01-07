/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.statistic.stat;

import com.actiontech.dble.server.parser.ServerParse;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author zhuam
 */
public class TableStat implements Comparable<TableStat> {

    //1 READ
    //2 WRITE
    //3 RELEATE
    //4 R/W TPS

    private String table;

    private final AtomicLong rCount = new AtomicLong(0);
    private final AtomicLong wCount = new AtomicLong(0);

    // relaTable
    private final ConcurrentMap<String, RelationTable> relationTableMap = new ConcurrentHashMap<>();

    private long lastExecuteTime;


    public TableStat(String table) {
        super();
        this.table = table;
    }

    public void reset() {
        this.rCount.set(0);
        this.wCount.set(0);
        this.relationTableMap.clear();
        this.lastExecuteTime = 0;
    }

    public void update(int sqlType, String sql, long startTime, long endTime, List<String> relationTables) {

        //RECORD RW
        switch (sqlType) {
            case ServerParse.SELECT:
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

        for (String tableName : relationTables) {
            RelationTable relationTable = this.relationTableMap.get(tableName);
            if (relationTable == null) {
                relationTable = new RelationTable(tableName, 1);
            } else {
                relationTable.incCount();
            }
            this.relationTableMap.put(tableName, relationTable);
        }

        this.lastExecuteTime = endTime;
    }

    public String getTable() {
        return table;
    }

    public long getRCount() {
        return this.rCount.get();
    }

    public long getWCount() {
        return this.wCount.get();
    }

    public int getCount() {
        return (int) (getRCount() + getWCount());
    }

    public List<RelationTable> getRelationTables() {
        List<RelationTable> tables = new ArrayList<>();
        tables.addAll(this.relationTableMap.values());
        return tables;
    }

    public long getLastExecuteTime() {
        return lastExecuteTime;
    }

    @Override
    public int compareTo(TableStat o) {
        long para = o.getCount() - getCount();
        long para2 = o.getLastExecuteTime() - getLastExecuteTime();
        return para == 0 ? (para2 == 0 ? o.getTable().hashCode() - getTable().hashCode() : (int) para2) : (int) para;
    }

    @Override
    public int hashCode() {
        long hash = getCount();
        hash = hash * 31 + getLastExecuteTime();
        hash = hash * 31 + getTable().hashCode();
        return (int) hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TableStat) {
            return this.compareTo((TableStat) obj) == 0;
        } else {
            return super.equals(obj);
        }
    }

    public void setTable(String table) {
        this.table = table;
    }

    /**
     * RelationTable
     *
     * @author Ben
     */
    public static class RelationTable {

        private String tableName;
        private int count;

        public RelationTable(String tableName, int count) {
            super();
            this.tableName = tableName;
            this.count = count;
        }

        public String getTableName() {
            return this.tableName;
        }

        public int getCount() {
            return this.count;
        }

        public void incCount() {
            this.count++;
        }
    }

}
