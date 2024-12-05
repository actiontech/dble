/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.meta.table;

import com.oceanbase.obsharding_d.alarm.AlarmCode;
import com.oceanbase.obsharding_d.alarm.Alert;
import com.oceanbase.obsharding_d.alarm.AlertUtil;
import com.oceanbase.obsharding_d.alarm.ToResolveContainer;
import com.oceanbase.obsharding_d.btrace.provider.GeneralProvider;
import com.oceanbase.obsharding_d.config.model.sharding.SchemaConfig;
import com.oceanbase.obsharding_d.config.model.sharding.table.BaseTableConfig;
import com.oceanbase.obsharding_d.config.model.sharding.table.ShardingTableFakeConfig;
import com.oceanbase.obsharding_d.meta.ReloadLogHelper;
import com.oceanbase.obsharding_d.util.CollectionUtil;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ConfigTableHandler extends ModeTableHandler {
    protected static final Logger LOGGER = LoggerFactory.getLogger(ConfigTableHandler.class);

    private final AbstractSchemaMetaHandler operationalHandler;
    protected final String schema;
    protected final SchemaConfig schemaConfig;
    private final Set<String> selfNodes;
    protected final ReloadLogHelper logger;
    private final Map<String, Map<String, List<String>>> tablesStructMap = new HashMap<>();
    protected final Set<String> filterTables;

    public ConfigTableHandler(AbstractSchemaMetaHandler operationalHandler) {
        this.operationalHandler = operationalHandler;
        this.schemaConfig = operationalHandler.getSchemaConfig();
        this.schema = operationalHandler.getSchema();
        this.selfNodes = operationalHandler.getSelfNode();
        this.logger = operationalHandler.getLogger();
        this.filterTables = operationalHandler.getFilterTables();
    }

    @Override
    public boolean loadMetaData() {
        Map<String, Set<String>> tableMap = new ShowTableHandler(this, selfNodes).tryGetTablesByNode();
        if (CollectionUtil.isEmpty(tableMap)) return false;
        new ShowCreateTableHandler(this).execute(tableMap);
        return true;
    }

    @Override
    public synchronized void handleTable(String table, String shardingNode, boolean isView, String sql) {
        operationalHandler.checkTableConsistent(tablesStructMap, table, shardingNode, sql);
    }

    @Override
    public void countdown(String shardingNode, Set<String> remainingTables) {
        if (remainingTables != null && remainingTables.size() > 0) {
            for (String table : remainingTables) {
                String tableLackKey = AlertUtil.getTableLackKey(shardingNode, table);
                String warnMsg = "Can't get table " + table + "'s config from shardingNode:" + shardingNode + "! Maybe the table is not initialized!";
                logger.warn(warnMsg);
                AlertUtil.alertSelf(AlarmCode.TABLE_LACK, Alert.AlertLevel.WARN, warnMsg, AlertUtil.genSingleLabel("TABLE", tableLackKey));
                ToResolveContainer.TABLE_LACK.add(tableLackKey);
            }
        }
        this.tryComplete(shardingNode, false);
    }

    @Override
    public void tryComplete(String shardingNode, boolean isLastShardingNode) {
        isLastShardingNode = isLastShardingNode(shardingNode);
        if (isLastShardingNode) {
            logger.info("explicit tables in schema[" + schema + "], last shardingNode[" + shardingNode + "] ");
            operationalHandler.tryToAddMetadata(tablesStructMap);
        }
    }

    // filterTables
    protected Map<String, BaseTableConfig> getFilterConfigTables(Map<String, BaseTableConfig> configTables, Set<String> filterTableSet) {
        Map<String, BaseTableConfig> newReload = new HashMap<>();
        if (filterTableSet == null) {
            for (Map.Entry<String, BaseTableConfig> entry : schemaConfig.getTables().entrySet())
                if (!(entry.getValue() instanceof ShardingTableFakeConfig)) {
                    newReload.put(entry.getKey(), entry.getValue());
                }
        } else {
            for (String table : filterTableSet) {
                boolean isFakeTable = schemaConfig.getTables().get(table) instanceof ShardingTableFakeConfig;
                if (schemaConfig.getTables().containsKey(table) && !isFakeTable) {
                    newReload.put(table, schemaConfig.getTables().get(table));
                } else if (!isFakeTable) {
                    logger.warn("reload table[" + schemaConfig.getName() + "." + table + "] metadata, but table doesn't exist");
                }
            }
        }
        return newReload;
    }

    protected void dealTableLack(String shardingNode, String table) {
        String tableLackKey = AlertUtil.getTableLackKey(shardingNode, table);
        String warnMsg = "Can't get table " + table + "'s config from shardingNode:" + shardingNode + "! Maybe the table is not initialized!";
        LOGGER.warn(warnMsg);
        AlertUtil.alertSelf(AlarmCode.TABLE_LACK, Alert.AlertLevel.WARN, warnMsg, AlertUtil.genSingleLabel("TABLE", tableLackKey));
        ToResolveContainer.TABLE_LACK.add(tableLackKey);
    }

    protected void realDealTableLack() {
    }

    // show table
    class ShowTableHandler {
        private final ConfigTableHandler parentHandler;
        private final Set<String> selfNodes;

        ShowTableHandler(ConfigTableHandler parentHandler, Set<String> selfNodes) {
            this.parentHandler = parentHandler;
            this.selfNodes = selfNodes;
        }

        protected Map<String, Set<String>> tryGetTablesByNode() {
            Map<String, Set<String>> shardingNodeMap = tryGetTables0();
            if (CollectionUtil.isEmpty(shardingNodeMap)) return null;
            if (parentHandler instanceof FakeConfigTableHandler)
                logger.infoList("try to execute show tables in [" + schema + "] default multi shardingNode:", getShardDNSet());
            else
                logger.infoList("try to execute show tables in [" + schema + "] config table's shardingNode:", getShardDNSet());
            Map<String, Set<String>> tableMap = Maps.newHashMap();
            for (Map.Entry<String, Set<String>> nodeInfo : shardingNodeMap.entrySet()) {
                String node = nodeInfo.getKey();
                if (selfNodes != null && selfNodes.contains(node)) {
                    logger.info("the Node " + node + " is a selfNode,count down");
                    parentHandler.countdown(node, null);
                    continue;
                }

                ShowTableByNodeUnitHandler unitHandler = new ShowTableByNodeUnitHandler(nodeInfo.getValue(), node);
                unitHandler.execute();
                Set<String> existTables = unitHandler.getTablesByNodeUnit();
                if (existTables.size() == 0) {
                    logger.info("the Node " + node + " has no exist table,count down");
                    parentHandler.countdown(node, null);
                    continue;
                }
                tableMap.put(node, existTables);
            }
            realDealTableLack();
            return tableMap;
        }

        private Map<String, Set<String>> tryGetTables0() {
            Map<String, Set<String>> shardingNodeMap = new HashMap<>();
            Map<String, BaseTableConfig> filterConfigTables = getFilterConfigTables(schemaConfig.getTables(), filterTables);
            for (Map.Entry<String, BaseTableConfig> entry : filterConfigTables.entrySet()) {
                for (String shardingNode : entry.getValue().getShardingNodes()) {
                    Set<String> tables = shardingNodeMap.get(shardingNode);
                    if (tables == null) {
                        tables = new HashSet<>();
                        shardingNodeMap.put(shardingNode, tables);
                        getShardDNSet().add(shardingNode);
                    }
                    tables.add(entry.getKey());
                }
            }
            return shardingNodeMap;

        }

        protected Map<String, BaseTableConfig> getFilterConfigTables(Map<String, BaseTableConfig> configTables, Set<String> pfilterTables) {
            return parentHandler.getFilterConfigTables(configTables, pfilterTables);
        }

        class ShowTableByNodeUnitHandler extends GetNodeTablesHandler {
            private final Set<String> expectedTables;
            private final Set<String> tables = new HashSet<>();

            ShowTableByNodeUnitHandler(Set<String> expectedTables, String node) {
                super(node);
                this.expectedTables = expectedTables;
            }

            private Set<String> getTablesByNodeUnit() {
                lock.lock();
                try {
                    while (!isFinished) {
                        notify.await();
                    }
                } catch (InterruptedException e) {
                    LOGGER.warn("getTablesByNodeUnit() is interrupted.");
                    return Collections.emptySet();
                } finally {
                    lock.unlock();
                }
                return tables;
            }

            @Override
            protected void handleTable(String table, String tableType) {
                if (expectedTables.contains(table)) {
                    tables.add(table);
                }
            }

            @Override
            protected void handleFinished() {
                GeneralProvider.showTableByNodeUnitHandlerFinished();
                if (expectedTables.size() == tables.size()) {
                    super.handleFinished();
                    return;
                }
                for (String table : expectedTables) {
                    if (tables.contains(table)) {
                        continue;
                    }
                    parentHandler.handleTable(table, shardingNode, false, null);
                    parentHandler.dealTableLack(shardingNode, table);
                }
                super.handleFinished();
            }
        }
    }

    // show create table
    class ShowCreateTableHandler {
        private final ConfigTableHandler parentHandler;

        ShowCreateTableHandler(ConfigTableHandler parentHandler) {
            this.parentHandler = parentHandler;
        }

        private void execute(Map<String, Set<String>> tableMap) {
            if (parentHandler instanceof FakeConfigTableHandler)
                logger.infoList("try to execute show create tables in [" + schema + "] default multi shardingNode:", getShardDNSet());
            else
                logger.infoList("try to execute show create tables in [" + schema + "] config table's shardingNode:", getShardDNSet());

            for (Map.Entry<String, Set<String>> entry : tableMap.entrySet()) {
                new ShowCreateTableByNodeUnitHandler(this, schema, logger.isReload()).execute(entry.getKey(), entry.getValue());
            }
        }

        private void countdown(String shardingNode, Set<String> remainingTables) {
            parentHandler.countdown(shardingNode, remainingTables);
        }

        private void handleTable(String shardingNode, String table, boolean isView, String sql) {
            parentHandler.handleTable(table, shardingNode, isView, sql);
        }

        class ShowCreateTableByNodeUnitHandler extends GetTableMetaHandler {
            private final ShowCreateTableHandler parentHandler;

            ShowCreateTableByNodeUnitHandler(ShowCreateTableHandler parentHandler, String schema, boolean isReload) {
                super(schema, isReload);
                this.parentHandler = parentHandler;
            }

            @Override
            void countdown(String shardingNode, Set<String> tables) {
                this.parentHandler.countdown(shardingNode, tables);
            }

            @Override
            void handleTable(String shardingNode, String table, boolean isView, String sql) {
                this.parentHandler.handleTable(shardingNode, table, isView, sql);
            }
        }
    }
}
