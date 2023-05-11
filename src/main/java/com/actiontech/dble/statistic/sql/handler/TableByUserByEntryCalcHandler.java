/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.statistic.sql.handler;

import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.services.manager.information.ManagerTableUtil;
import com.actiontech.dble.statistic.sql.StatisticEvent;
import com.actiontech.dble.statistic.sql.StatisticManager;
import com.actiontech.dble.statistic.sql.entry.FrontendInfo;
import com.actiontech.dble.statistic.sql.entry.StatisticEntry;
import com.actiontech.dble.statistic.sql.entry.StatisticFrontendSqlEntry;

import java.util.*;

public class TableByUserByEntryCalcHandler implements StatisticDataHandler {

    Map<String, Record> records = new LinkedHashMap<>(1000);

    @Override
    public void onEvent(StatisticEvent statisticEvent, long l, boolean b) throws Exception {
        if (StatisticManager.getInstance().isEnable()) {
            handle(statisticEvent.getEntry());
        }
    }

    public void checkEliminate() {
        synchronized (records) {
            int removeIndex;
            if ((removeIndex = records.values().size() - StatisticManager.getInstance().getTableByUserByEntryTableSize()) > 0) {
                Iterator<String> iterator = records.keySet().iterator();
                while (removeIndex-- > 0) {
                    iterator.next();
                    iterator.remove();
                }
            }
        }
    }

    public void handle(StatisticEntry entry) {
        synchronized (records) {
            if (entry instanceof StatisticFrontendSqlEntry) {
                StatisticFrontendSqlEntry fEntry = ((StatisticFrontendSqlEntry) entry);
                Set<String> tableSet = new HashSet<>(ManagerTableUtil.getTables(fEntry.getSchema(), fEntry.getSql()));
                if (tableSet.isEmpty()) {
                    // dual, no table
                    toRecord("null", fEntry);
                } else {
                    for (String t : tableSet) {
                        toRecord(t, fEntry);
                    }
                }
            }
        }
    }

    private void toRecord(String table, StatisticFrontendSqlEntry fEntry) {
        if (fEntry.getSqlType() == 4 || fEntry.getSqlType() == 11 || fEntry.getSqlType() == 3 || fEntry.getSqlType() == 7) {
            String key = fEntry.getFrontend().getUserId() + "-" + fEntry.getFrontend().getUser() + "-" + table;
            Record currRecord = records.get(key);
            boolean isNew = currRecord == null;
            if (isNew) {
                checkEliminate();
                currRecord = new Record(fEntry.getFrontend().getUserId(), fEntry.getFrontend(), table);
            }
            switch (fEntry.getSqlType()) {
                case ServerParse.INSERT:
                    currRecord.addInsert(fEntry.getRows(), fEntry.getDuration());
                    break;
                case ServerParse.UPDATE:
                    currRecord.addUpdate(fEntry.getRows(), fEntry.getDuration());
                    break;
                case ServerParse.DELETE:
                    currRecord.addDelete(fEntry.getRows(), fEntry.getDuration());
                    break;
                case ServerParse.SELECT:
                    currRecord.addSelect(fEntry.getExaminedRows(), fEntry.getRows(), fEntry.getDuration());
                    break;
                default:
                    // ignore
                    break;
            }
            if (isNew) {
                records.put(key, currRecord);
            }
        }
    }


    @Override
    public Map<String, Record> getList() {
        checkEliminate();
        return new HashMap<>(records);
    }

    @Override
    public void clear() {
        synchronized (records) {
            records.clear();
        }
    }

    public static class Record {
        int entry;
        String user;
        String table;
        int insertCount = 0;
        long insertRows = 0L;
        long insertTime = 0L;

        int updateCount = 0;
        long updateRows = 0L;
        long updateTime = 0L;

        int deleteCount = 0;
        long deleteRows = 0L;
        long deleteTime = 0L;

        int selectCount = 0;
        long selectRows = 0L;
        long selectExaminedRowsRows = 0L;
        long selectTime = 0L;

        long lastUpdateTime = 0L;

        public Record(int entry, FrontendInfo frontend, String table) {
            this.entry = entry;
            user = frontend.getUser();
            this.table = table;
        }

        public void addInsert(long row, long time) {
            insertCount += 1;
            insertRows += row;
            insertTime += time;
            lastUpdateTime = System.currentTimeMillis();
        }

        public void addUpdate(long row, long time) {
            updateCount += 1;
            updateRows += row;
            updateTime += time;
            lastUpdateTime = System.currentTimeMillis();
        }

        public void addDelete(long row, long time) {
            deleteCount += 1;
            deleteRows += row;
            deleteTime += time;
            lastUpdateTime = System.currentTimeMillis();
        }

        public void addSelect(long examinedRowsRow, long row, long time) {
            selectCount += 1;
            selectExaminedRowsRows += examinedRowsRow;
            selectRows += row;
            selectTime += time;
            lastUpdateTime = System.currentTimeMillis();
        }

        public int getEntry() {
            return entry;
        }

        public long getUpdateTime() {
            return updateTime;
        }

        public int getInsertCount() {
            return insertCount;
        }

        public long getInsertRows() {
            return insertRows;
        }

        public long getInsertTime() {
            return insertTime;
        }

        public int getUpdateCount() {
            return updateCount;
        }

        public long getUpdateRows() {
            return updateRows;
        }

        public int getDeleteCount() {
            return deleteCount;
        }

        public long getDeleteRows() {
            return deleteRows;
        }

        public long getDeleteTime() {
            return deleteTime;
        }

        public int getSelectCount() {
            return selectCount;
        }

        public long getSelectRows() {
            return selectRows;
        }

        public long getSelectTime() {
            return selectTime;
        }

        public long getLastUpdateTime() {
            return lastUpdateTime;
        }

        public String getUser() {
            return user;
        }

        public String getTable() {
            return table;
        }

        public long getSelectExaminedRowsRows() {
            return selectExaminedRowsRows;
        }
    }
}
