/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.statistic.sql.handler;

import com.oceanbase.obsharding_d.services.manager.information.ManagerTableUtil;
import com.oceanbase.obsharding_d.statistic.sql.StatisticEvent;
import com.oceanbase.obsharding_d.statistic.sql.StatisticManager;
import com.oceanbase.obsharding_d.statistic.sql.entry.FrontendInfo;
import com.oceanbase.obsharding_d.statistic.sql.entry.StatisticEntry;
import com.oceanbase.obsharding_d.statistic.sql.entry.StatisticFrontendSqlEntry;

import java.util.*;

public class AssociateTablesByEntryByUserCalcHandler implements StatisticDataHandler {

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
            if ((removeIndex = records.values().size() - StatisticManager.getInstance().getAssociateTablesByEntryByUserTableSize()) > 0) {
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
                if (fEntry.getSqlType() == 7) {
                    List<String> tableList = ManagerTableUtil.getTables(fEntry.getSchema(), fEntry.getSql());
                    if (!tableList.isEmpty() && tableList.size() > 1) {
                        Collections.sort(tableList);
                        String tables = String.join(",", tableList);
                        String key = fEntry.getFrontend().getUserId() + "-" + fEntry.getFrontend().getUser() + "-" + tables;
                        Record currRecord = records.get(key);
                        boolean isNew = currRecord == null;
                        if (isNew) {
                            checkEliminate();
                            currRecord = new Record(fEntry.getFrontend().getUserId(), fEntry.getFrontend(), tables);
                        }
                        currRecord.addSelect(fEntry.getExaminedRows().longValue(), fEntry.getRows(), fEntry.getDuration());
                        if (isNew) {
                            records.put(key, currRecord);
                        }
                    }
                }
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
        String tables;

        int selectCount = 0;
        long selectRows = 0L;
        long selectExaminedRowsRows = 0L;
        long selectTime = 0L;
        long lastUpdateTime = 0L;

        public Record(int entry, FrontendInfo frontend, String tables) {
            this.entry = entry;
            user = frontend.getUser();
            this.tables = tables;
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

        public String getUser() {
            return user;
        }

        public String getTables() {
            return tables;
        }

        public int getSelectCount() {
            return selectCount;
        }

        public long getSelectRows() {
            return selectRows;
        }

        public long getSelectExaminedRowsRows() {
            return selectExaminedRowsRows;
        }

        public long getSelectTime() {
            return selectTime;
        }

        public long getLastUpdateTime() {
            return lastUpdateTime;
        }
    }
}
