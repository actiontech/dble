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

    private static final String COLUMN_NAME = "name";

    private static final String COLUMN_SHARDING_NODE = "sharding_node";

    private static final String COLUMN_SQL_MAX_LIMIT = "sql_max_limit";

    public DbleSchema() {
        super(TABLE_NAME, 3);
    }

    @Override
    protected void initColumnAndType() {
        columns.put(COLUMN_NAME, new ColumnMeta(COLUMN_NAME, "varchar(64)", false, true));
        columnsType.put(COLUMN_NAME, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_SHARDING_NODE, new ColumnMeta(COLUMN_SHARDING_NODE, "varchar(64)", true));
        columnsType.put(COLUMN_SHARDING_NODE, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_SQL_MAX_LIMIT, new ColumnMeta(COLUMN_SQL_MAX_LIMIT, "int(11)", true));
        columnsType.put(COLUMN_SQL_MAX_LIMIT, Fields.FIELD_TYPE_LONG);

    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        Map<String, SchemaConfig> schemaMap = new TreeMap<>(DbleServer.getInstance().getConfig().getSchemas());
        return schemaMap.values().stream().map(e -> {
            LinkedHashMap<String, String> map = Maps.newLinkedHashMap();
            map.put(COLUMN_NAME, e.getName());
            map.put(COLUMN_SHARDING_NODE, e.getShardingNode());
            map.put(COLUMN_SQL_MAX_LIMIT, String.valueOf(e.getDefaultMaxLimit()));
            return map;
        }).collect(Collectors.toList());
    }
}
