/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.manager.information;

import com.actiontech.dble.manager.information.tables.*;

import java.util.Map;
import java.util.TreeMap;

public final class ManagerSchemaInfo {
    public static final String SCHEMA_NAME = "dble_information";
    private static final ManagerSchemaInfo INSTANCE = new ManagerSchemaInfo();

    private Map<String, ManagerBaseTable> tables = new TreeMap<>();

    private ManagerSchemaInfo() {
        //todo :delete demotest1 and demotest2
        registerTable(new DemoTest1());
        registerTable(new DemoTest2());

        registerTable(new Version());
        registerTable(new DbleVariables());
        registerTable(new DbleThreadPool());
        registerTable(new DbleFrontConnections());
        registerTable(new DbleBackendConnections());
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
