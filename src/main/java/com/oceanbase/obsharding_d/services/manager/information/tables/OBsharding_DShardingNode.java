/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.information.tables;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.datasource.ShardingNode;
import com.oceanbase.obsharding_d.config.Fields;
import com.oceanbase.obsharding_d.meta.ColumnMeta;
import com.oceanbase.obsharding_d.services.manager.information.ManagerBaseTable;
import com.oceanbase.obsharding_d.singleton.PauseShardingNodeManager;
import com.google.common.collect.Maps;

import java.util.*;
import java.util.stream.Collectors;

public class OBsharding_DShardingNode extends ManagerBaseTable {

    public static final String TABLE_NAME = "obsharding_d_sharding_node";

    private static final String COLUMN_NAME = "name";

    public static final String COLUMN_DB_GROUP = "db_group";

    private static final String COLUMN_DB_SCHEMA = "db_schema";

    private static final String COLUMN_PAUSE = "pause";

    public OBsharding_DShardingNode() {
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
        Map<String, ShardingNode> shardingNodeMap = new TreeMap<>(OBsharding_DServer.getInstance().getConfig().getShardingNodes());
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
