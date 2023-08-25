/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.statistic.sql.analyzer;

import com.actiontech.dble.server.parser.AbstractServerParse;
import com.actiontech.dble.services.manager.information.ManagerTableUtil;
import com.actiontech.dble.statistic.sql.entry.StatisticFrontendSqlEntry;
import com.actiontech.dble.util.CollectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * TableStatAnalyzer
 *
 * @author zhuam
 */
public final class TableStatAnalyzer implements AbstractAnalyzer {

    private static final Logger LOGGER = LoggerFactory.getLogger(TableStatAnalyzer.class);

    private Map<String, TableStat> tableStatMap = new ConcurrentHashMap<>();
    private ReentrantLock lock = new ReentrantLock();

    private static final TableStatAnalyzer INSTANCE = new TableStatAnalyzer();

    private TableStatAnalyzer() {
    }

    public static TableStatAnalyzer getInstance() {
        return INSTANCE;
    }

    @Override
    public void toAnalyzing(StatisticFrontendSqlEntry fEntry) {
        AbstractServerParse.BusinessType businessType = AbstractServerParse.getBusinessType(fEntry.getSqlType());
        switch (businessType) {
            case R:
            case W:
                List<String> tableList = new ArrayList<>(fEntry.getTables());
                if (CollectionUtil.isEmpty(tableList) ||
                        (tableList.size() > 1 && tableList.size() != tableList.stream().distinct().count())) {
                    tableList = ManagerTableUtil.getTables(fEntry.getSchema(), fEntry.getSql());
                }
                String masterTable = null;
                if (tableList.size() >= 1) {
                    masterTable = tableList.get(0);
                }
                if (masterTable != null) {
                    tableList.remove(0);
                    TableStat tableStat = getTableStat(masterTable);
                    tableStat.update(businessType, fEntry.getEndTimeMs(), tableList);
                }
                break;
            default:
                break;
        }
    }

    private TableStat getTableStat(String tableName) {
        TableStat userStat = tableStatMap.get(tableName);
        if (userStat == null) {
            if (lock.tryLock()) {
                try {
                    userStat = new TableStat(tableName);
                    tableStatMap.put(tableName, userStat);
                } finally {
                    lock.unlock();
                }
            } else {
                while (userStat == null) {
                    userStat = tableStatMap.get(tableName);
                }
            }
        }
        return userStat;
    }

    public List<TableStat> getTableStats(boolean isClear) {
        SortedSet<TableStat> tableStatSortedSet = new TreeSet<>(tableStatMap.values());
        List<TableStat> list = new ArrayList<>(tableStatSortedSet);
        if (isClear) {
            tableStatMap = new ConcurrentHashMap<>();
        }
        return list;
    }

    public void clearTable() {
        tableStatMap.clear();
    }

}
