/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.meta.table;

import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.alarm.ToResolveContainer;

import java.util.Map;
import java.util.Set;

public class ConfigTableMetaHandler extends GetTableMetaHandler {

    private Set<String> selfNode;
    private AbstractSchemaMetaHandler schemaMetaHandler;

    public ConfigTableMetaHandler(AbstractSchemaMetaHandler schemaMetaHandler, String schema, Set<String> selfNode, boolean isReload) {
        super(schema, isReload);
        this.selfNode = selfNode;
        this.schemaMetaHandler = schemaMetaHandler;
    }

    public void execute(Map<String, Set<String>> shardingNodeMap) {
        for (Map.Entry<String, Set<String>> shardingNodeInfo : shardingNodeMap.entrySet()) {
            String shardingNode = shardingNodeInfo.getKey();
            if (selfNode != null && selfNode.contains(shardingNode)) {
                logger.info("the Node " + shardingNode + " is a selfNode,count down");
                this.countdown(shardingNode, null);
                continue;
            }

            Set<String> existTables = listExistTables(shardingNode, shardingNodeInfo.getValue());
            if (existTables.size() == 0) {
                logger.info("the Node " + shardingNode + " has no exist table,count down");
                this.countdown(shardingNode, null);
                continue;
            }
            super.execute(shardingNode, existTables);
        }
    }

    @Override
    void handleTable(String shardingNode, String table, boolean isView, String sql) {
        schemaMetaHandler.checkTableConsistent(table, shardingNode, sql);
    }

    @Override
    void countdown(String shardingNode, Set<String> remainingTables) {
        if (remainingTables != null && remainingTables.size() > 0) {
            for (String table : remainingTables) {
                String tableId = "sharding_node[" + shardingNode + "]:Table[" + table + "]";
                String warnMsg = "Can't get table " + table + "'s config from shardingNode:" + shardingNode + "! Maybe the table is not initialized!";
                logger.warn(warnMsg);
                AlertUtil.alertSelf(AlarmCode.TABLE_LACK, Alert.AlertLevel.WARN, warnMsg, AlertUtil.genSingleLabel("TABLE", tableId));
                ToResolveContainer.TABLE_LACK.add(tableId);
            }
        }
        schemaMetaHandler.countDownShardTable(shardingNode);
    }

    private Set<String> listExistTables(String shardingNode, Set<String> tables) {
        GetConfigTablesHandler showTablesHandler = new GetConfigTablesHandler(tables, shardingNode, this);
        showTablesHandler.execute();
        return showTablesHandler.getExistsTables();
    }

}
