/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.information.tables;

import com.oceanbase.obsharding_d.buffer.MemoryBufferMonitor;
import com.oceanbase.obsharding_d.config.Fields;
import com.oceanbase.obsharding_d.meta.ColumnMeta;
import com.oceanbase.obsharding_d.services.manager.information.ManagerBaseTable;
import com.google.common.collect.Lists;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;

public class OBsharding_DMemoryResident extends ManagerBaseTable {

    private static final String TABLE_NAME = "obsharding_d_memory_resident";

    private static final String COLUMN_ID = "id";

    private static final String COLUMN_ALIVE_SECOND = "alive_second";

    private static final String COLUMN_STACKTRACE = "stacktrace";

    private static final String COLUMN_BUFFER_TYPE = "buffer_type";

    private static final String COLUMN_ALLOCATE_TIME = "allocate_time";

    private static final String COLUMN_ALLOCATE_SIZE = "allocate_size";

    private static final String COLUMN_SQL = "sql";


    private static final String KEY_CLASS = "class";

    public OBsharding_DMemoryResident() {
        super(TABLE_NAME, 5);
    }

    @Override
    protected void initColumnAndType() {
        columns.put(COLUMN_ID, new ColumnMeta(COLUMN_ID, "bigint", false, true));
        columnsType.put(COLUMN_ID, Fields.FIELD_TYPE_LONGLONG);

        columns.put(COLUMN_ALIVE_SECOND, new ColumnMeta(COLUMN_ALIVE_SECOND, "double", false));
        columnsType.put(COLUMN_ALIVE_SECOND, Fields.FIELD_TYPE_DOUBLE);

        columns.put(COLUMN_STACKTRACE, new ColumnMeta(COLUMN_STACKTRACE, "text", false));
        columnsType.put(COLUMN_STACKTRACE, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_BUFFER_TYPE, new ColumnMeta(COLUMN_BUFFER_TYPE, "varchar(64)", false));
        columnsType.put(COLUMN_BUFFER_TYPE, Fields.FIELD_TYPE_VAR_STRING);


        columns.put(COLUMN_ALLOCATE_SIZE, new ColumnMeta(COLUMN_ALLOCATE_SIZE, "int(11)", false, false));
        columnsType.put(COLUMN_ALLOCATE_SIZE, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_ALLOCATE_TIME, new ColumnMeta(COLUMN_ALLOCATE_TIME, "timestamp(3)", false));
        columnsType.put(COLUMN_ALLOCATE_TIME, Fields.FIELD_TYPE_TIMESTAMP);

        columns.put(COLUMN_SQL, new ColumnMeta(COLUMN_SQL, "text", false));
        columnsType.put(COLUMN_SQL, Fields.FIELD_TYPE_VAR_STRING);


    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        List<LinkedHashMap<String, String>> rowList = Lists.newLinkedList();
        final long currentTime = System.currentTimeMillis();
        MemoryBufferMonitor.getInstance().recordForEach((key, bqr) -> {
            LinkedHashMap<String, String> row = new LinkedHashMap<>();
            row.put(COLUMN_ID, String.valueOf(key));
            final double costTime = ((double) (currentTime - bqr.getAllocatedTime())) / 1000;
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
            row.put(COLUMN_ALIVE_SECOND, String.valueOf(costTime));
            row.put(COLUMN_STACKTRACE, str.toString());
            row.put(COLUMN_BUFFER_TYPE, bqr.getType().toString());
            row.put(COLUMN_ALLOCATE_SIZE, String.valueOf(bqr.getAllocateSize()));
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            row.put(COLUMN_ALLOCATE_TIME, sdf.format(new Date(bqr.getAllocatedTime())));
            row.put(COLUMN_SQL, bqr.getSql());
            rowList.add(row);
        });


        return rowList;
    }

}
