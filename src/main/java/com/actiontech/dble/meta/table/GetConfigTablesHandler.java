/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.meta.table;

import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.alarm.ToResolveContainer;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class GetConfigTablesHandler extends GetNodeTablesHandler {

    private final Set<String> expectedTables;
    private final ConfigTableMetaHandler metaHandler;
    private final Set<String> existsTables = new HashSet<>();

    GetConfigTablesHandler(Set<String> expectedTables, String shardingNode, ConfigTableMetaHandler metaHandler) {
        super(shardingNode);
        this.expectedTables = expectedTables;
        this.metaHandler = metaHandler;
    }

    Set<String> getExistsTables() {
        lock.lock();
        try {
            while (!isFinished) {
                notify.await();
            }
        } catch (InterruptedException e) {
            LOGGER.warn("getExistsTables() is interrupted.");
            return Collections.emptySet();
        } finally {
            lock.unlock();
        }
        return existsTables;
    }

    @Override
    protected void handleTable(String table, String tableType) {
        if (expectedTables.contains(table)) {
            existsTables.add(table);
        }
    }

    @Override
    protected void handleFinished() {
        if (expectedTables.size() == existsTables.size()) {
            super.handleFinished();
            return;
        }
        for (String table : expectedTables) {
            if (existsTables.contains(table)) {
                continue;
            }
            metaHandler.handleTable(shardingNode, table, false, null);
            String tableId = "sharding_node[" + shardingNode + "]:Table[" + table + "]";
            String warnMsg = "Can't get table " + table + "'s config from shardingNode:" + shardingNode + "! Maybe the table is not initialized!";
            LOGGER.warn(warnMsg);
            AlertUtil.alertSelf(AlarmCode.TABLE_LACK, Alert.AlertLevel.WARN, warnMsg, AlertUtil.genSingleLabel("TABLE", tableId));
            ToResolveContainer.TABLE_LACK.add(tableId);
        }
        super.handleFinished();
    }
}
