/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.information.tables;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.ApNode;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.services.manager.information.ManagerBaseTable;
import com.google.common.collect.Maps;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class DbleApNode extends ManagerBaseTable {

    public static final String TABLE_NAME = "dble_ap_node";

    private static final String COLUMN_NAME = "name";

    public static final String COLUMN_DB_GROUP = "db_group";

    private static final String COLUMN_DB_SCHEMA = "db_schema";

    public DbleApNode() {
        super(TABLE_NAME, 3);
    }

    @Override
    protected void initColumnAndType() {
        columns.put(COLUMN_NAME, new ColumnMeta(COLUMN_NAME, "varchar(64)", false, true));
        columnsType.put(COLUMN_NAME, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_DB_GROUP, new ColumnMeta(COLUMN_DB_GROUP, "varchar(64)", false));
        columnsType.put(COLUMN_DB_GROUP, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_DB_SCHEMA, new ColumnMeta(COLUMN_DB_SCHEMA, "varchar(64)", false));
        columnsType.put(COLUMN_DB_SCHEMA, Fields.FIELD_TYPE_VAR_STRING);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        Map<String, ApNode> apNodeMap = new TreeMap<>(DbleServer.getInstance().getConfig().getApNodes());
        return apNodeMap.values().stream().map(e -> {
            LinkedHashMap<String, String> map = Maps.newLinkedHashMap();
            map.put(COLUMN_NAME, e.getName());
            map.put(COLUMN_DB_GROUP, e.getDbGroupName());
            map.put(COLUMN_DB_SCHEMA, e.getDatabase());
            return map;
        }).collect(Collectors.toList());
    }
}
