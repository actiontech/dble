/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.statistic;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * SQLRecorder
 *
 * @author mycat
 */
public final class SQLRecorder {

    private final int count;
    SortedSet<SQLRecord> records;

    public SQLRecorder(int count) {
        this.count = count;
        this.records = new ConcurrentSkipListSet<>();
    }

    public List<SQLRecord> getRecords() {
        List<SQLRecord> keyList = new ArrayList<>(records);
        return keyList;
    }


    public void add(SQLRecord record) {
        records.add(record);
    }

    public void clear() {
        records.clear();
    }

    public void recycle() {
        if (records.size() > count) {
            SortedSet<SQLRecord> records2 = new ConcurrentSkipListSet<>();
            List<SQLRecord> keyList = new ArrayList<>(records);
            int i = 0;
            for (SQLRecord key : keyList) {
                if (i == count) {
                    break;
                }
                records2.add(key);
                i++;
            }
            records = records2;
        }
    }
}
