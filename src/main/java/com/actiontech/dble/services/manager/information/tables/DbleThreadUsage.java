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

    private static final String COLUMN_1 = "thread_name";

    private static final String COLUMN_2 = "last_quarter_min";

    private static final String COLUMN_3 = "last_minute";

    private static final String COLUMN_4 = "last_five_minute";

    public DbleThreadUsage() {
        super(TABLE_NAME, 4);
    }

    @Override
    protected void initColumnAndType() {
        columns.put(COLUMN_1, new ColumnMeta(COLUMN_1, "varchar(64)", false, true));
        columnsType.put(COLUMN_1, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_2, new ColumnMeta(COLUMN_2, "varchar(5)", false));
        columnsType.put(COLUMN_2, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_3, new ColumnMeta(COLUMN_3, "varchar(5)", false));
        columnsType.put(COLUMN_3, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_4, new ColumnMeta(COLUMN_4, "varchar(5)", false));
        columnsType.put(COLUMN_4, Fields.FIELD_TYPE_VAR_STRING);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        Map<String, ThreadWorkUsage> threadUsedMap = new TreeMap<>(DbleServer.getInstance().getThreadUsedMap());
        return threadUsedMap.entrySet().stream().map(e -> {
            LinkedHashMap<String, String> map = Maps.newLinkedHashMap();
            String[] usedPercent = e.getValue().getUsedPercent();
            map.put(COLUMN_1, e.getKey());
            map.put(COLUMN_2, null != usedPercent && usedPercent.length > 0 ? usedPercent[0] : null);
            map.put(COLUMN_3, null != usedPercent && usedPercent.length > 1 ? usedPercent[1] : null);
            map.put(COLUMN_4, null != usedPercent && usedPercent.length > 2 ? usedPercent[2] : null);
            return map;
        }).collect(Collectors.toList());
    }
}
