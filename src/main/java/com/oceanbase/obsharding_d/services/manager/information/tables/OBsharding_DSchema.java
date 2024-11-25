/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.information.tables;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.config.Fields;
import com.oceanbase.obsharding_d.config.model.sharding.SchemaConfig;
import com.oceanbase.obsharding_d.meta.ColumnMeta;
import com.oceanbase.obsharding_d.services.manager.information.ManagerBaseTable;
import com.google.common.collect.Maps;

import java.util.*;
import java.util.stream.Collectors;

public class OBsharding_DSchema extends ManagerBaseTable {

    private static final String TABLE_NAME = "obsharding-d_schema";

    private static final String COLUMN_NAME = "name";

    private static final String COLUMN_SHARDING_NODE = "sharding_node";

    private static final String COLUMN_FUNCTION = "function";

    private static final String COLUMN_SQL_MAX_LIMIT = "sql_max_limit";

    private static final String COLUMN_LOGICAL_CREATE_AND_DROP = "logical_create_and_drop";

    public OBsharding_DSchema() {
        super(TABLE_NAME, 5);
    }

    @Override
    protected void initColumnAndType() {
        columns.put(COLUMN_NAME, new ColumnMeta(COLUMN_NAME, "varchar(64)", false, true));
        columnsType.put(COLUMN_NAME, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_SHARDING_NODE, new ColumnMeta(COLUMN_SHARDING_NODE, "varchar(64)", true));
        columnsType.put(COLUMN_SHARDING_NODE, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_FUNCTION, new ColumnMeta(COLUMN_FUNCTION, "varchar(64)", true));
        columnsType.put(COLUMN_FUNCTION, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_SQL_MAX_LIMIT, new ColumnMeta(COLUMN_SQL_MAX_LIMIT, "int(11)", true));
        columnsType.put(COLUMN_SQL_MAX_LIMIT, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_LOGICAL_CREATE_AND_DROP, new ColumnMeta(COLUMN_LOGICAL_CREATE_AND_DROP, "varchar(5)", true));
        columnsType.put(COLUMN_LOGICAL_CREATE_AND_DROP, Fields.FIELD_TYPE_VAR_STRING);

    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        Map<String, SchemaConfig> schemaMap = new TreeMap<>(OBsharding_DServer.getInstance().getConfig().getSchemas());
        return schemaMap.values().stream().map(e -> {
            LinkedHashMap<String, String> map = Maps.newLinkedHashMap();
            map.put(COLUMN_NAME, e.getName());
            map.put(COLUMN_SHARDING_NODE, e.getDefaultShardingNodes() == null ? null : String.join(",", e.getDefaultShardingNodes()));
            map.put(COLUMN_FUNCTION, e.getFunction() == null ? "-" : e.getFunction().getName());
            map.put(COLUMN_SQL_MAX_LIMIT, String.valueOf(e.getDefaultMaxLimit()));
            map.put(COLUMN_LOGICAL_CREATE_AND_DROP, e.isLogicalCreateADrop() + "");
            return map;
        }).collect(Collectors.toList());
    }
}
