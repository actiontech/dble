/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.meta.table;

import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.alarm.ToResolveContainer;

import java.util.HashSet;
import java.util.Set;

public class GetSpecialNodeTablesHandler extends GetNodeTablesHandler {

    private AbstractTablesMetaHandler handler;
    private Set<String> tables;
    private volatile Set<String> existsTables = new HashSet<>();
    private volatile boolean finished = false;
    GetSpecialNodeTablesHandler(AbstractTablesMetaHandler handler, Set<String> tables, String dataNode) {
        super(dataNode);
        this.handler = handler;
        this.tables = tables;
    }


    public Set<String> getExistsTables() {
        return existsTables;
    }

    @Override
    protected void handleTables(String table) {
        if (tables.contains(table)) {
            existsTables.add(table);
        }
    }

    @Override
    protected void handleFinished() {
        if (tables.size() != existsTables.size()) {
            for (String table : tables) {
                if (!existsTables.contains(table)) {
                    String tableId = "DataNode[" + dataNode + "]:Table[" + table + "]";
                    String warnMsg = "Can't get table " + table + "'s config from DataNode:" + dataNode + "! Maybe the table is not initialized!";
                    LOGGER.warn(warnMsg);
                    AlertUtil.alertSelf(AlarmCode.TABLE_LACK, Alert.AlertLevel.WARN, warnMsg, AlertUtil.genSingleLabel("TABLE", tableId));
                    ToResolveContainer.TABLE_LACK.add(tableId);
                    handler.handlerTable(table, dataNode, null);
                }
            }
        }
        finished = true;
        handler.showTablesFinished();
    }
    public boolean isFinished() {
        return finished;
    }
}
