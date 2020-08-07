/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.information.tables;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.services.manager.information.ManagerBaseTable;
import com.google.common.collect.Maps;

import java.util.*;
import java.util.stream.Collectors;

public class DbleSchema extends ManagerBaseTable {

    private static final String TABLE_NAME = "dble_schema";

    private static final String COLUMN_1 = "name";

    private static final String COLUMN_2 = "sharding_node";

    private static final String COLUMN_3 = "sql_max_limit";

    public DbleSchema() {
        super(TABLE_NAME, 3);
    }

    @Override
    protected void initColumnAndType() {
        columns.put(COLUMN_1, new ColumnMeta(COLUMN_1, "varchar(64)", false, true));
        columnsType.put(COLUMN_1, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_2, new ColumnMeta(COLUMN_2, "varchar(64)", true));
        columnsType.put(COLUMN_2, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_3, new ColumnMeta(COLUMN_3, "int(11)", true));
        columnsType.put(COLUMN_3, Fields.FIELD_TYPE_LONG);

    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        Map<String, SchemaConfig> schemaMap = new TreeMap<>(DbleServer.getInstance().getConfig().getSchemas());
        return schemaMap.values().stream().map(e -> {
            LinkedHashMap<String, String> map = Maps.newLinkedHashMap();
            map.put(COLUMN_1, e.getName());
            map.put(COLUMN_2, e.getShardingNode());
            map.put(COLUMN_3, String.valueOf(e.getDefaultMaxLimit()));
            return map;
        }).collect(Collectors.toList());
    }
}
