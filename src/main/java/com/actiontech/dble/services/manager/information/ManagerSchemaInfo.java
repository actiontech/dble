/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.information;

import com.actiontech.dble.services.manager.information.tables.*;

import java.util.Map;
import java.util.TreeMap;

public final class ManagerSchemaInfo {
    public static final String SCHEMA_NAME = "dble_information";
    private static final ManagerSchemaInfo INSTANCE = new ManagerSchemaInfo();

    private Map<String, ManagerBaseTable> tables = new TreeMap<>();

    private ManagerSchemaInfo() {
        registerTable(new Version());
        registerTable(new DbleVariables());
        registerTable(new DbleThreadPool());
        registerTable(new DbleFrontConnections());
        registerTable(new DbleBackendConnections());
        registerTable(new DbleShardingNode());
        registerTable(new DbleSchema());
        registerTable(new DbleThreadUsage());
        registerTable(new DbleEntry());
        registerTable(new DbleEntrySchema());
        registerTable(new DbleEntryDbGroup());
        registerTable(new DbleRwSplitEntry());
        registerTable(new DbleEntryTablePrivilege());
        registerTable(new DbleStatus());
        registerTable(new DbleProcessor());
        registerTable(new DbleBlacklist());
        registerTable(new DbleReloadStatus());
        registerTable(new DbleXaSession());
        registerTable(new DbleDdlLock());
        registerTable(new DbleTable());
        registerTable(new DbleGlobalTable());
        registerTable(new DbleShardingTable());
        registerTable(new DbleChildTable());
        registerTable(new DbleTableShardingNode());
        registerTable(new DbleDbGroup());
        registerTable(new DbleDbInstance());
        registerTable(new DbleAlgorithm());
        registerTable(new ProcessList());
        registerTable(new SessionVariables());
        registerTable(new BackendVariables());
    }


    private void registerTable(ManagerBaseTable table) {
        tables.put(table.getTableName(), table);
    }

    public static ManagerSchemaInfo getInstance() {
        return INSTANCE;
    }

    public Map<String, ManagerBaseTable> getTables() {
        return tables;
    }
}
