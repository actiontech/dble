/*
 * Copyright (C) 2016-2018 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.meta.table;

import com.actiontech.dble.alarm.*;
import com.actiontech.dble.server.status.AlertManager;

import java.util.HashSet;
import java.util.Set;

public class GetSpecialNodeTablesHandler extends GetNodeTablesHandler {

    private AbstractTablesMetaHandler handler;
    private Set<String> tables;
    private volatile Set<String> existsTables = new HashSet<>();

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
                    final String tableId = "DataNode[" + dataNode + "]:Table[" + table + "]";
                    final String warnMsg = "Can't get table " + table + "'s config from DataNode:" + dataNode + "! Maybe the table is not initialized!";
                    LOGGER.warn(warnMsg);
                    AlertManager.getInstance().getAlertQueue().offer(new AlertTask() {
                        @Override
                        public void send() {
                            AlertUtil.alertSelf(AlarmCode.TABLE_LACK, Alert.AlertLevel.WARN, warnMsg, AlertUtil.genSingleLabel("TABLE", tableId));
                        }

                        @Override
                        public String toString() {
                            return "AlertManager Task alertSelf " + AlarmCode.TABLE_LACK + " " + warnMsg + " " + tableId;
                        }
                    });
                    ToResolveContainer.TABLE_LACK.add(tableId);
                }
            }
        }
        handler.showTablesFinished();
    }
}
