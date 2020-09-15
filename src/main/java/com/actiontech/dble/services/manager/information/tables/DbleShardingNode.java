/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.information.tables;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.ShardingNode;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.services.manager.information.ManagerBaseTable;
import com.actiontech.dble.singleton.PauseShardingNodeManager;
import com.google.common.collect.Maps;

import java.util.*;
import java.util.stream.Collectors;

public class DbleShardingNode extends ManagerBaseTable {

    public static final String TABLE_NAME = "dble_sharding_node";

    private static final String COLUMN_NAME = "name";

    public static final String COLUMN_DB_GROUP = "db_group";

    private static final String COLUMN_DB_SCHEMA = "db_schema";

    private static final String COLUMN_PAUSE = "pause";

    public DbleShardingNode() {
        super(TABLE_NAME, 4);
    }

    @Override
    protected void initColumnAndType() {
        columns.put(COLUMN_NAME, new ColumnMeta(COLUMN_NAME, "varchar(64)", false, true));
        columnsType.put(COLUMN_NAME, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_DB_GROUP, new ColumnMeta(COLUMN_DB_GROUP, "varchar(64)", false));
        columnsType.put(COLUMN_DB_GROUP, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_DB_SCHEMA, new ColumnMeta(COLUMN_DB_SCHEMA, "varchar(64)", false));
        columnsType.put(COLUMN_DB_SCHEMA, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_PAUSE, new ColumnMeta(COLUMN_PAUSE, "varchar(5)", true));
        columnsType.put(COLUMN_PAUSE, Fields.FIELD_TYPE_VAR_STRING);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        Set<String> pauseNodeSet = PauseShardingNodeManager.getInstance().getShardingNodes();
        Map<String, ShardingNode> shardingNodeMap = new TreeMap<>(DbleServer.getInstance().getConfig().getShardingNodes());
        return shardingNodeMap.values().stream().map(e -> {
            LinkedHashMap<String, String> map = Maps.newLinkedHashMap();
            map.put(COLUMN_NAME, e.getName());
            map.put(COLUMN_DB_GROUP, e.getDbGroupName());
            map.put(COLUMN_DB_SCHEMA, e.getDatabase());
            map.put(COLUMN_PAUSE, null != pauseNodeSet && pauseNodeSet.contains(e.getName()) ? "true" : "false");
            return map;
        }).collect(Collectors.toList());
    }
}
