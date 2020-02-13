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

    public void execute(Map<String, Set<String>> dataNodeMap) {
        for (Map.Entry<String, Set<String>> dataNodeInfo : dataNodeMap.entrySet()) {
            String dataNode = dataNodeInfo.getKey();
            if (selfNode != null && selfNode.contains(dataNode)) {
                logger.info("the Node " + dataNode + " is a selfNode,count down");
                this.countdown(dataNode, null);
                continue;
            }

            Set<String> existTables = listExistTables(dataNode, dataNodeInfo.getValue());
            if (existTables.size() == 0) {
                logger.info("the Node " + dataNode + " has no exist table,count down");
                this.countdown(dataNode, null);
                continue;
            }
            super.execute(dataNode, existTables);
        }
    }

    @Override
    void handleTable(String dataNode, String table, boolean isView, String sql) {
        schemaMetaHandler.checkTableConsistent(table, dataNode, sql);
    }

    @Override
    void countdown(String dataNode, Set<String> remainingTables) {
        if (remainingTables != null && remainingTables.size() > 0) {
            for (String table : remainingTables) {
                String tableId = "DataNode[" + dataNode + "]:Table[" + table + "]";
                String warnMsg = "Can't get table " + table + "'s config from DataNode:" + dataNode + "! Maybe the table is not initialized!";
                logger.warn(warnMsg);
                AlertUtil.alertSelf(AlarmCode.TABLE_LACK, Alert.AlertLevel.WARN, warnMsg, AlertUtil.genSingleLabel("TABLE", tableId));
                ToResolveContainer.TABLE_LACK.add(tableId);
            }
        }
        schemaMetaHandler.countDownShardTable(dataNode);
    }

    private Set<String> listExistTables(String dataNode, Set<String> tables) {
        GetConfigTablesHandler showTablesHandler = new GetConfigTablesHandler(tables, dataNode, this);
        showTablesHandler.execute();
        return showTablesHandler.getExistsTables();
    }

}
