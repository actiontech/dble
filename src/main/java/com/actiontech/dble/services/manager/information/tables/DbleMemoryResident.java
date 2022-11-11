/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.information.tables;

import com.actiontech.dble.buffer.MemoryBufferMonitor;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.services.manager.information.ManagerBaseTable;
import com.google.common.collect.Lists;

import java.util.LinkedHashMap;
import java.util.List;

public class DbleMemoryResident extends ManagerBaseTable {

    private static final String TABLE_NAME = "dble_memory_resident";

    private static final String COLUMN_ID = "id";

    private static final String COLUMN_LIVE_SECOND = "live_second";

    private static final String COLUMN_STACKTRACE = "stacktrace";

    private static final String COLUMN_ALLOCATE_TYPE = "allocate_type";

    private static final String COLUMN_ALLOCATE_LENGTH = "allocate_length";

    private static final String COLUMN_SQL = "sql";


    private static final String KEY_CLASS = "class";

    public DbleMemoryResident() {
        super(TABLE_NAME, 5);
    }

    @Override
    protected void initColumnAndType() {
        columns.put(COLUMN_ID, new ColumnMeta(COLUMN_ID, "varchar(20)", false, true));
        columnsType.put(COLUMN_ID, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_LIVE_SECOND, new ColumnMeta(COLUMN_LIVE_SECOND, "double", false));
        columnsType.put(COLUMN_LIVE_SECOND, Fields.FIELD_TYPE_DOUBLE);

        columns.put(COLUMN_STACKTRACE, new ColumnMeta(COLUMN_STACKTRACE, "text", false));
        columnsType.put(COLUMN_STACKTRACE, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_ALLOCATE_TYPE, new ColumnMeta(COLUMN_ALLOCATE_TYPE, "varchar(64)", false));
        columnsType.put(COLUMN_ALLOCATE_TYPE, Fields.FIELD_TYPE_VAR_STRING);


        columns.put(COLUMN_ALLOCATE_LENGTH, new ColumnMeta(COLUMN_ALLOCATE_LENGTH, "int(11)", false, false));
        columnsType.put(COLUMN_ALLOCATE_LENGTH, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_SQL, new ColumnMeta(COLUMN_SQL, "text", false));
        columnsType.put(COLUMN_SQL, Fields.FIELD_TYPE_VAR_STRING);


    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        List<LinkedHashMap<String, String>> rowList = Lists.newLinkedList();
        final long currentTime = System.currentTimeMillis();
        MemoryBufferMonitor.getInstance().recordForEach((key, bqr) -> {
            LinkedHashMap<String, String> row = new LinkedHashMap<>();
            row.put(COLUMN_ID, Integer.toHexString(key));
            final double costTime = currentTime - bqr.getAllocatedTime();
            //only cost time>1s will print
            if (costTime < 1) {
                return;
            }
            StringBuilder str = new StringBuilder();
            for (String stacktrace : bqr.getStacktrace()) {
                str.append("\n");
                str.append(stacktrace);
            }
            str.append("\n");
            row.put(COLUMN_LIVE_SECOND, String.valueOf(costTime / 1000));
            row.put(COLUMN_STACKTRACE, str.toString());
            row.put(COLUMN_ALLOCATE_TYPE, bqr.getType().toString());
            row.put(COLUMN_ALLOCATE_LENGTH, String.valueOf(bqr.getAllocateLength()));
            row.put(COLUMN_SQL, bqr.getSql());
            rowList.add(row);
        });


        return rowList;
    }

}