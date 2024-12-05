/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.statistic.sql.handler;

import com.oceanbase.obsharding_d.server.parser.ServerParse;
import com.oceanbase.obsharding_d.statistic.sql.StatisticEvent;
import com.oceanbase.obsharding_d.statistic.sql.StatisticManager;
import com.oceanbase.obsharding_d.statistic.sql.entry.FrontendInfo;
import com.oceanbase.obsharding_d.statistic.sql.entry.StatisticBackendSqlEntry;
import com.oceanbase.obsharding_d.statistic.sql.entry.StatisticEntry;
import com.oceanbase.obsharding_d.statistic.sql.entry.StatisticTxEntry;

import java.util.*;
import java.util.stream.Collectors;

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
            if (entry instanceof StatisticTxEntry) {
                Set<String> keys = new HashSet<>();
                ((StatisticTxEntry) entry).getEntryList().
                        stream().
                        flatMap(k -> k.getBackendSqlEntrys().values().stream()).
                        collect(Collectors.toList()).
                        stream().
                        forEach(v -> {
                            String key = v.getKey();
                            keys.add(key);
                            Record currRecord;
                            boolean isNew = false;
                            if (isNew = ((currRecord = records.get(key)) == null)) {
                                checkEliminate();
                                currRecord = new Record(entry.getFrontend().getUserId(), entry.getFrontend(), v.getBackend());
                            }
                            if (v.getAllEndTime() != 0) {
                                currRecord.addTxRows(v.getRows());
                            }
                            if (isNew) {
                                records.put(key, currRecord);
                            }
                        });
                keys.stream().forEach(k -> {
                    Record currRecord = records.get(k);
                    if (currRecord != null) {
                        currRecord.addTx(entry.getDuration());
                    }
                });
            } else if (entry instanceof StatisticBackendSqlEntry) {
                StatisticBackendSqlEntry backendSqlEntry = (StatisticBackendSqlEntry) entry;
                String key = backendSqlEntry.getKey();
                Record currRecord = records.get(key);
                boolean isNew = currRecord == null;
                if (isNew) {
                    checkEliminate();
                    currRecord = new Record(entry.getFrontend().getUserId(), entry.getFrontend(), backendSqlEntry.getBackend());
                }
                if (backendSqlEntry.isNeedToTx()) {
                    currRecord.addTxRows(backendSqlEntry.getRows());
                    currRecord.addTx(entry.getDuration());
                }
                if (backendSqlEntry.getSqlType() == 4 || backendSqlEntry.getSqlType() == 11 || backendSqlEntry.getSqlType() == 3 || backendSqlEntry.getSqlType() == 7) {
                    switch (backendSqlEntry.getSqlType()) {
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
        StatisticEntry.BackendInfo backend;

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

        public Record(int entry, FrontendInfo frontend, StatisticEntry.BackendInfo backend) {
            this.entry = entry;
            this.frontend = frontend;
            this.backend = backend;
        }

        public void addTx(long row, long time) {
            txCount += 1;
            txRows += row;
            txTime += time;
            lastUpdateTime = System.currentTimeMillis();
        }

        public void addTx(long time) {
            txCount += 1;
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

        public StatisticEntry.BackendInfo getBackend() {
            return backend;
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

        public void setBackend(StatisticEntry.BackendInfo backend) {
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
