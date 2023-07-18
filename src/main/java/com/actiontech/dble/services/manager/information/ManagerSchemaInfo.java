/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.information;

import com.actiontech.dble.services.manager.information.tables.*;
import com.actiontech.dble.services.manager.information.tables.statistic.AssociateTablesByEntryByUser;
import com.actiontech.dble.services.manager.information.tables.statistic.FrontendByBackendByEntryByUser;
import com.actiontech.dble.services.manager.information.tables.statistic.SqlLog;
import com.actiontech.dble.services.manager.information.tables.statistic.TableByUserByEntry;
import com.actiontech.dble.services.manager.information.views.SqlLogByDigestByEntryByUser;
import com.actiontech.dble.services.manager.information.views.SqlLogByTxByEntryByUser;
import com.actiontech.dble.services.manager.information.views.SqlLogByTxDigestByEntryByUser;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public final class ManagerSchemaInfo {
    public static final String SCHEMA_NAME = "dble_information";
    private static final ManagerSchemaInfo INSTANCE = new ManagerSchemaInfo();

    static {
        INSTANCE.initViews();
    }

    private Map<String, ManagerBaseTable> tables = new TreeMap<>();
    private Map<String, ManagerBaseView> views = new HashMap<>(8);

    private ManagerSchemaInfo() {
        registerTable(new Version());
        registerTable(new DbleVariables());
        registerTable(new DbleThreadPool());
        registerTable(new DbleThreadPoolTask());
        registerTable(new DbleFrontConnections());
        registerTable(new DbleBackendConnections());
        registerTable(new DbleShardingNode());
        registerTable(new DbleApNode());
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
        registerTable(new DbleConfig());
        registerTable(new FrontendByBackendByEntryByUser());
        registerTable(new TableByUserByEntry());
        registerTable(new AssociateTablesByEntryByUser());
        registerTable(new DbleXaRecover());
        // sampling
        registerTable(new SqlLog());

        registerTable(new DbleFlowControl());
        registerTable(new DbleFrontConnectionsActiveRatio());
        registerTable(new DbleFrontConnectionsAssociateThread());
        registerTable(new DbleBackendConnectionsAssociateThread());
        registerTable(new DbleClusterRenewThread());
        registerTable(new RecyclingResource());
        registerTable(new DbleDelayDetection());
        registerTable(new DbleMemoryResident());
    }

    private void initViews() {
        registerView(new SqlLogByTxByEntryByUser());
        registerView(new SqlLogByDigestByEntryByUser());
        registerView(new SqlLogByTxDigestByEntryByUser());
    }

    private void registerTable(ManagerBaseTable table) {
        tables.put(table.getTableName(), table);
    }

    private void registerView(ManagerBaseView view) {
        views.put(view.getViewName(), view);
    }

    public static ManagerSchemaInfo getInstance() {
        return INSTANCE;
    }

    public Map<String, ManagerBaseTable> getTables() {
        return tables;
    }

    public Map<String, ManagerBaseView> getViews() {
        return views;
    }

    public ManagerBaseView getView(String viewName) {
        return views.get(viewName);
    }
}
