/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.information.tables;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.services.manager.information.ManagerBaseTable;
import com.actiontech.dble.statistic.stat.ThreadWorkUsage;
import com.google.common.collect.Maps;

import java.util.*;
import java.util.stream.Collectors;

public class DbleThreadUsage extends ManagerBaseTable {

    private static final String TABLE_NAME = "dble_thread_usage";

    private static final String COLUMN_THREAD_NAME = "thread_name";

    private static final String COLUMN_LAST_QUARTER_MIN = "last_quarter_min";

    private static final String COLUMN_LAST_MINUTE = "last_minute";

    private static final String COLUMN_LAST_FIVE_MINUTE = "last_five_minute";

    public DbleThreadUsage() {
        super(TABLE_NAME, 4);
    }

    @Override
    protected void initColumnAndType() {
        columns.put(COLUMN_THREAD_NAME, new ColumnMeta(COLUMN_THREAD_NAME, "varchar(64)", false, true));
        columnsType.put(COLUMN_THREAD_NAME, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_LAST_QUARTER_MIN, new ColumnMeta(COLUMN_LAST_QUARTER_MIN, "varchar(5)", false));
        columnsType.put(COLUMN_LAST_QUARTER_MIN, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_LAST_MINUTE, new ColumnMeta(COLUMN_LAST_MINUTE, "varchar(5)", false));
        columnsType.put(COLUMN_LAST_MINUTE, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_LAST_FIVE_MINUTE, new ColumnMeta(COLUMN_LAST_FIVE_MINUTE, "varchar(5)", false));
        columnsType.put(COLUMN_LAST_FIVE_MINUTE, Fields.FIELD_TYPE_VAR_STRING);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        Map<String, ThreadWorkUsage> threadUsedMap = new TreeMap<>(DbleServer.getInstance().getThreadUsedMap());
        return threadUsedMap.entrySet().stream().map(e -> {
            LinkedHashMap<String, String> map = Maps.newLinkedHashMap();
            String[] usedPercent = e.getValue().getUsedPercent();
            map.put(COLUMN_THREAD_NAME, e.getKey());
            map.put(COLUMN_LAST_QUARTER_MIN, null != usedPercent && usedPercent.length > 0 ? usedPercent[0] : null);
            map.put(COLUMN_LAST_MINUTE, null != usedPercent && usedPercent.length > 1 ? usedPercent[1] : null);
            map.put(COLUMN_LAST_FIVE_MINUTE, null != usedPercent && usedPercent.length > 2 ? usedPercent[2] : null);
            return map;
        }).collect(Collectors.toList());
    }
}
