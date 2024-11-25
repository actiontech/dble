/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.information;

import com.oceanbase.obsharding_d.services.manager.information.tables.*;
import com.oceanbase.obsharding_d.services.manager.information.tables.statistic.AssociateTablesByEntryByUser;
import com.oceanbase.obsharding_d.services.manager.information.tables.statistic.FrontendByBackendByEntryByUser;
import com.oceanbase.obsharding_d.services.manager.information.tables.statistic.SqlLog;
import com.oceanbase.obsharding_d.services.manager.information.tables.statistic.TableByUserByEntry;
import com.oceanbase.obsharding_d.services.manager.information.views.SqlLogByDigestByEntryByUser;
import com.oceanbase.obsharding_d.services.manager.information.views.SqlLogByTxByEntryByUser;
import com.oceanbase.obsharding_d.services.manager.information.views.SqlLogByTxDigestByEntryByUser;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public final class ManagerSchemaInfo {
    public static final String SCHEMA_NAME = "obsharding-d_information";
    private static final ManagerSchemaInfo INSTANCE = new ManagerSchemaInfo();

    static {
        INSTANCE.initViews();
    }

    private Map<String, ManagerBaseTable> tables = new TreeMap<>();
    private Map<String, ManagerBaseView> views = new HashMap<>(8);

    private ManagerSchemaInfo() {
        registerTable(new Version());
        registerTable(new OBsharding_DVariables());
        registerTable(new OBsharding_DThreadPool());
        registerTable(new OBsharding_DThreadPoolTask());
        registerTable(new OBsharding_DFrontConnections());
        registerTable(new OBsharding_DBackendConnections());
        registerTable(new OBsharding_DShardingNode());
        registerTable(new OBsharding_DSchema());
        registerTable(new OBsharding_DThreadUsage());
        registerTable(new OBsharding_DEntry());
        registerTable(new OBsharding_DEntrySchema());
        registerTable(new OBsharding_DEntryDbGroup());
        registerTable(new OBsharding_DRwSplitEntry());
        registerTable(new OBsharding_DEntryTablePrivilege());
        registerTable(new OBsharding_DStatus());
        registerTable(new OBsharding_DProcessor());
        registerTable(new OBsharding_DBlacklist());
        registerTable(new OBsharding_DReloadStatus());
        registerTable(new OBsharding_DXaSession());
        registerTable(new OBsharding_DDdlLock());
        registerTable(new OBsharding_DTable());
        registerTable(new OBsharding_DGlobalTable());
        registerTable(new OBsharding_DShardingTable());
        registerTable(new OBsharding_DChildTable());
        registerTable(new OBsharding_DTableShardingNode());
        registerTable(new OBsharding_DDbGroup());
        registerTable(new OBsharding_DDbInstance());
        registerTable(new OBsharding_DAlgorithm());
        registerTable(new ProcessList());
        registerTable(new SessionVariables());
        registerTable(new BackendVariables());
        registerTable(new OBsharding_DConfig());
        registerTable(new FrontendByBackendByEntryByUser());
        registerTable(new TableByUserByEntry());
        registerTable(new AssociateTablesByEntryByUser());
        registerTable(new OBsharding_DXaRecover());
        // sampling
        registerTable(new SqlLog());

        registerTable(new OBsharding_DFlowControl());
        registerTable(new OBsharding_DFrontConnectionsActiveRatio());
        registerTable(new OBsharding_DFrontConnectionsAssociateThread());
        registerTable(new OBsharding_DBackendConnectionsAssociateThread());
        registerTable(new OBsharding_DClusterRenewThread());
        registerTable(new RecyclingResource());
        registerTable(new OBsharding_DDelayDetection());
        registerTable(new OBsharding_DMemoryResident());
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
