/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.information.tables;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.sharding.table.BaseTableConfig;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.services.manager.information.ManagerBaseTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

public class DbleTableShardingNode extends ManagerBaseTable {

    private static final String TABLE_NAME = "dble_table_sharding_node";

    private static final String COLUMN_ID = "id";

    private static final String COLUMN_SHARDING_NODE = "sharding_node";

    private static final String COLUMN_ORDER = "order";

    public DbleTableShardingNode() {
        super(TABLE_NAME, 3);
    }

    @Override
    protected void initColumnAndType() {
        columns.put(COLUMN_ID, new ColumnMeta(COLUMN_ID, "varchar(64)", false, true));
        columnsType.put(COLUMN_ID, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_SHARDING_NODE, new ColumnMeta(COLUMN_SHARDING_NODE, "varchar(32)", false, true));
        columnsType.put(COLUMN_SHARDING_NODE, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_ORDER, new ColumnMeta(COLUMN_ORDER, "int(11)", false));
        columnsType.put(COLUMN_ORDER, Fields.FIELD_TYPE_LONG);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        List<LinkedHashMap<String, String>> rowList = Lists.newLinkedList();
        List<String> nameList = Lists.newArrayList();
        TreeMap<String, SchemaConfig> schemaMap = new TreeMap<>(DbleServer.getInstance().getConfig().getSchemas());
        schemaMap.entrySet().
                stream().
                forEach(e -> e.getValue().getTables().entrySet().
                        stream().
                        sorted((a, b) -> Integer.valueOf(a.getValue().getId()).compareTo(b.getValue().getId())).
                        forEach(t -> {
                            BaseTableConfig baseTableConfig = t.getValue();
                            List<String> shardingNodes = baseTableConfig.getShardingNodes();
                            AtomicInteger index = new AtomicInteger();
                            String id = DbleTable.PREFIX_CONFIG + baseTableConfig.getId();
                            shardingNodes.
                                    stream().
                                    filter(q -> !nameList.contains(id + "-" + q)).
                                    forEach(p -> {
                                        LinkedHashMap<String, String> map = Maps.newLinkedHashMap();
                                        map.put(COLUMN_ID, id);
                                        map.put(COLUMN_SHARDING_NODE, p);
                                        map.put(COLUMN_ORDER, String.valueOf(index.getAndIncrement()));
                                        rowList.add(map);
                                        nameList.add(id + "-" + p);
                                    });
                        }));
        return rowList;
    }
}
