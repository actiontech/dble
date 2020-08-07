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

    private static final String TABLE_NAME = "dble_sharding_node";

    private static final String COLUMN_1 = "name";

    private static final String COLUMN_2 = "db_group";

    private static final String COLUMN_3 = "db_schema";

    private static final String COLUMN_4 = "pause";

    public DbleShardingNode() {
        super(TABLE_NAME, 4);
    }

    @Override
    protected void initColumnAndType() {
        columns.put(COLUMN_1, new ColumnMeta(COLUMN_1, "varchar(64)", false, true));
        columnsType.put(COLUMN_1, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_2, new ColumnMeta(COLUMN_2, "varchar(64)", false));
        columnsType.put(COLUMN_2, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_3, new ColumnMeta(COLUMN_3, "varchar(64)", false));
        columnsType.put(COLUMN_3, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_4, new ColumnMeta(COLUMN_4, "varchar(5)", true));
        columnsType.put(COLUMN_4, Fields.FIELD_TYPE_VAR_STRING);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        Set<String> pauseNodeSet = PauseShardingNodeManager.getInstance().getShardingNodes();
        Map<String, ShardingNode> shardingNodeMap = new TreeMap<>(DbleServer.getInstance().getConfig().getShardingNodes());
        return shardingNodeMap.values().stream().map(e -> {
            LinkedHashMap<String, String> map = Maps.newLinkedHashMap();
            map.put(COLUMN_1, e.getName());
            map.put(COLUMN_2, e.getDbGroupName());
            map.put(COLUMN_3, e.getDatabase());
            map.put(COLUMN_4, null != pauseNodeSet && pauseNodeSet.contains(e.getName()) ? "true" : "false");
            return map;
        }).collect(Collectors.toList());
    }
}
