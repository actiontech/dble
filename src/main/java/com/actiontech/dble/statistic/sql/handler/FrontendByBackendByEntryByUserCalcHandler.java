/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.statistic.sql.handler;

import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.statistic.sql.StatisticEvent;
import com.actiontech.dble.statistic.sql.StatisticManager;
import com.actiontech.dble.statistic.sql.entry.BackendInfo;
import com.actiontech.dble.statistic.sql.entry.FrontendInfo;
import com.actiontech.dble.statistic.sql.entry.StatisticBackendSqlEntry;
import com.actiontech.dble.statistic.sql.entry.StatisticEntry;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class FrontendByBackendByEntryByUserCalcHandler implements StatisticDataHandler {

    Map<String, Record> records = new LinkedHashMap<>(1000);

    @Override
    public void onEvent(StatisticEvent statisticEvent, long l, boolean b) {
        if (StatisticManager.getInstance().isEnable()) {
            handle(statisticEvent.getEntry());
        }
    }

    public void checkEliminate() {
        synchronized (records) {
            int removeIndex;
            if ((removeIndex = records.values().size() - StatisticManager.getInstance().getFrontendByBackendByEntryByUserTableSize()) > 0) {
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
            if (entry instanceof StatisticBackendSqlEntry) {
                StatisticBackendSqlEntry backendSqlEntry = (StatisticBackendSqlEntry) entry;
                String key = backendSqlEntry.getKey();
                Record currRecord = records.get(key);
                boolean isNew = currRecord == null;
                if (isNew) {
                    checkEliminate();
                    currRecord = new Record(entry.getFrontend().getUserId(), entry.getFrontend(), backendSqlEntry.getBackend(), backendSqlEntry.getNode());
                }
                int sqlType = backendSqlEntry.getSqlType();
                if (sqlType == 4 || sqlType == 11 || sqlType == 3 || sqlType == 7) {
                    switch (sqlType) {
                        case ServerParse.INSERT:
                            currRecord.addInsert(backendSqlEntry.getRows(), backendSqlEntry.getDuration());
                            break;
                        case ServerParse.UPDATE:
                            currRecord.addUpdate(backendSqlEntry.getRows(), backendSqlEntry.getDuration());
                            break;
                        case ServerParse.DELETE:
                            currRecord.addDelete(backendSqlEntry.getRows(), backendSqlEntry.getDuration());
                            break;
                        case ServerParse.SELECT:
                            currRecord.addSelect(backendSqlEntry.getRows(), backendSqlEntry.getDuration());
                            break;
                        default:
                            // ignore
                            break;
                    }
                    currRecord.addTxRows(backendSqlEntry.getRows());
                    currRecord.addTx(entry.getDuration());
                }
                if (backendSqlEntry.isNeedToTx()) {
                    currRecord.incrementTx();
                }
                if (isNew) {
                    records.put(key, currRecord);
                }
            }
        }
    }

    @Override
    public void clear() {
        synchronized (records) {
            records.clear();
        }
    }

    @Override
    public Map<String, Record> getList() {
        checkEliminate();
        return new HashMap<>(records);
    }

    // sql_statistic_by_frontend_by_backend_by_entry_by_user
    public static class Record {
        int entry;
        FrontendInfo frontend;
        BackendInfo backend;
        String node;

        int txCount = 0;
        long txRows = 0L;
        long txTime = 0L;

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
        long selectTime = 0L;

        long lastUpdateTime = 0L;

        public Record(int entry, FrontendInfo frontend, BackendInfo backend, String node) {
            this.entry = entry;
            this.frontend = frontend;
            this.backend = backend;
            this.node = node;
        }

        public void incrementTx() {
            txCount += 1;
            lastUpdateTime = System.currentTimeMillis();
        }

        public void addTx(long time) {
            txTime += time;
            lastUpdateTime = System.currentTimeMillis();
        }

        public void addTxRows(long row) {
            txRows += row;
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

        public void addSelect(long row, long time) {
            selectCount += 1;
            selectRows += row;
            selectTime += time;
            lastUpdateTime = System.currentTimeMillis();
        }

        public int getEntry() {
            return entry;
        }

        public FrontendInfo getFrontend() {
            return frontend;
        }

        public BackendInfo getBackend() {
            return backend;
        }

        public String getNode() {
            return node;
        }

        public long getUpdateTime() {
            return updateTime;
        }

        public void setEntry(int entry) {
            this.entry = entry;
        }

        public void setFrontend(FrontendInfo frontend) {
            this.frontend = frontend;
        }

        public void setBackend(BackendInfo backend) {
            this.backend = backend;
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

        public void setSelectTime(long selectTime) {
            this.selectTime = selectTime;
        }

        public long getLastUpdateTime() {
            return lastUpdateTime;
        }

        public void setLastUpdateTime(long lastUpdateTime) {
            this.lastUpdateTime = lastUpdateTime;
        }

        public int getTxCount() {
            return txCount;
        }

        public long getTxRows() {
            return txRows;
        }

        public long getTxTime() {
            return txTime;
        }
    }
}
