package com.actiontech.dble.statistic.sql.handler;

import com.actiontech.dble.services.manager.information.ManagerTableUtil;
import com.actiontech.dble.statistic.sql.StatisticEvent;
import com.actiontech.dble.statistic.sql.StatisticManager;
import com.actiontech.dble.statistic.sql.entry.FrontendInfo;
import com.actiontech.dble.statistic.sql.entry.StatisticEntry;
import com.actiontech.dble.statistic.sql.entry.StatisticFrontendSqlEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class TableByUserByEntryCalcHandler implements StatisticDataHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableByUserByEntryCalcHandler.class);

    // key：
    Map<String, Record> records = new LinkedHashMap<>(1024);
    int entryId = 0;

    @Override
    public void onEvent(StatisticEvent statisticEvent, long l, boolean b) throws Exception {
        // LOGGER.info("consuming:{}", event.getEntry().toString());
        check();
        handle(statisticEvent.getEntry());
    }

    public void check() {
        synchronized (records) {
            int removeIndex;
            if ((removeIndex = records.values().size() - StatisticManager.getInstance().getStatisticTableSize()) > 0) {
                Iterator<String> iterator = records.keySet().iterator();
                while (removeIndex-- > 0) {
                    iterator.next();
                    iterator.remove();
                }
            }
        }
    }

    public void handle(StatisticEntry entry) {
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

    private void toRecord(String table, StatisticFrontendSqlEntry fEntry) {
        String key = fEntry.getFrontend().getUser() + "-" + table;
        Record currRecord;
        boolean isNew = true;
        if (isNew = ((currRecord = records.get(key)) == null)) {
            currRecord = new Record(++entryId, fEntry.getFrontend(), table);
        }
        switch (fEntry.getSqlType()) {
            case 4:
                currRecord.addInsert(fEntry.getRows(), fEntry.getDuration());
                break;
            case 11:
                currRecord.addUpdate(fEntry.getRows(), fEntry.getDuration());
                break;
            case 3:
                currRecord.addDelete(fEntry.getRows(), fEntry.getDuration());
                break;
            case 7:
                currRecord.addSelect(fEntry.getExaminedRows().longValue(), fEntry.getRows(), fEntry.getDuration());
                break;
            default:
                // ignore
                break;
        }
        if (isNew) {
            records.put(key, currRecord);
        }
    }


    @Override
    public Map<String, Record> getList() {
        return new HashMap<>(records);
    }

    public String getKey() {
        return "";
    }


    @Override
    public void clear() {
        synchronized (records) {
            records.clear();
            entryId = 0;
        }
    }

    public static class Record {
        int entry;
        String user;
        String table;
        int insertCount = 0;
        long insertRows = 0;
        long insertTime = 0L;

        int updateCount = 0;
        long updateRows = 0;
        long updateTime = 0L;

        int deleteCount = 0;
        long deleteRows = 0;
        long deleteTime = 0L;

        int selectCount = 0;
        long selectRows = 0;
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
