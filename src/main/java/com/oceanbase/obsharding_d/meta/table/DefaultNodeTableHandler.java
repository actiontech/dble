/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.meta.table;

import com.oceanbase.obsharding_d.config.model.sharding.SchemaConfig;
import com.oceanbase.obsharding_d.config.model.sharding.table.BaseTableConfig;
import com.oceanbase.obsharding_d.meta.ReloadLogHelper;
import com.oceanbase.obsharding_d.meta.TableMeta;
import com.oceanbase.obsharding_d.meta.ViewMeta;
import com.oceanbase.obsharding_d.util.CollectionUtil;
import com.google.common.collect.Sets;

import java.util.*;

public class DefaultNodeTableHandler extends ModeTableHandler {
    private final AbstractSchemaMetaHandler operationalHandler;
    private final String schema;
    private final SchemaConfig schemaConfig;
    private final Set<String> selfNodes;
    private final ReloadLogHelper logger;
    private final Map<String, Map<String, List<String>>> tablesStructMap = new HashMap<>();
    private final Set<String> filterTables;

    public DefaultNodeTableHandler(AbstractSchemaMetaHandler operationalHandler) {
        this.operationalHandler = operationalHandler;
        this.schemaConfig = operationalHandler.getSchemaConfig();
        this.schema = operationalHandler.getSchema();
        this.selfNodes = operationalHandler.getSelfNode();
        this.logger = operationalHandler.getLogger();
        this.filterTables = operationalHandler.getFilterTables();
    }

    @Override
    public boolean loadMetaData() {
        boolean existTable = false;
        if (!CollectionUtil.isEmpty(schemaConfig.getDefaultShardingNodes()) &&
                (selfNodes == null || Collections.disjoint(selfNodes, schemaConfig.getDefaultShardingNodes()))) {
            Set<String> showTablesResult = new ShowTableHandler().tryGetTables();
            if (!CollectionUtil.isEmpty(filterTables) && schemaConfig.isDefaultSingleNode()) {
                showTablesResult.retainAll(filterTables);
                filterTables.removeAll(showTablesResult);
            }

            if (showTablesResult.size() > 0) {
                existTable = true;
                getShardDNSet().addAll(schemaConfig.getDefaultShardingNodes());
                new ShowCreateTableHandler(this).
                        execute(showTablesResult);
            }
        }
        return existTable;
    }

    @Override
    public synchronized void handleTable(String shardingNode, String table, boolean isView, String sql) {
        if (schemaConfig.isDefaultSingleNode()) {
            if (isView) {
                ViewMeta viewMeta = MetaHelper.initViewMeta(schema, sql, System.currentTimeMillis(), operationalHandler.getTmManager());
                operationalHandler.handleViewMeta(viewMeta);
            } else {
                TableMeta tableMeta = MetaHelper.initTableMeta(table, sql, System.currentTimeMillis(), schema);
                operationalHandler.handleSingleMetaData(tableMeta);
            }
        } else {
            if (isView) return;
            operationalHandler.checkTableConsistent(tablesStructMap, table, shardingNode, sql);
        }
    }

    @Override
    public void countdown(String shardingNode, Set<String> remainingTables) {
        boolean isLastShardingNode = isLastShardingNode(shardingNode);
        if (schemaConfig.isDefaultSingleNode()) {
            if (remainingTables.size() > 0) {
                for (String table : remainingTables) {
                    logger.warn("show create table " + table + " in shardingNode[" + shardingNode + "] has no results");
                }
            }
            this.tryComplete(shardingNode, isLastShardingNode);
        } else {
            if (isLastShardingNode) {
                if (remainingTables.size() > 0) {
                    for (String table : remainingTables) {
                        logger.warn("show create table " + table + " in shardingNode" + schemaConfig.getDefaultShardingNodes() + " has no intersection results");
                    }
                }
            }
            this.tryComplete(shardingNode, isLastShardingNode);
        }
    }

    @Override
    public void tryComplete(String shardingNode, boolean isLastShardingNode) {
        if (isLastShardingNode) {
            logger.info("implicit defaultNode tables in schema[" + schema + "], last shardingNode[" + shardingNode + "] ");
            operationalHandler.tryToAddMetadata(tablesStructMap);
        }
    }

    // show table
    class ShowTableHandler {
        private List<ShowTableByNodeUnitHandler> nodeUnitHandlers;

        ShowTableHandler() {
        }

        private Set<String> tryGetTables() {
            List<String> nodes;
            if ((nodes = schemaConfig.getDefaultShardingNodes()) == null) return Sets.newHashSet();
            logger.infoList("try to execute show tables in [" + schema + "] default shardingNode:", new HashSet<>(schemaConfig.getDefaultShardingNodes()));
            this.nodeUnitHandlers = new ArrayList<>(nodes.size());
            for (String shardingNode : nodes) {
                ShowTableByNodeUnitHandler showTablesHandler = new ShowTableByNodeUnitHandler(shardingNode, schemaConfig);
                nodeUnitHandlers.add(showTablesHandler);
                showTablesHandler.execute();
            }
            return getAllNodeTables();
        }

        private Set<String> getAllNodeTables() {
            if (nodeUnitHandlers.size() > 1) {
                List<Set<String>> tablesSets = new ArrayList<>();
                for (ShowTableByNodeUnitHandler handler : nodeUnitHandlers) {
                    tablesSets.add(handler.getTablesByNodeUnit());
                }
                return CollectionUtil.retainAll(tablesSets);
            } else {
                return nodeUnitHandlers.get(0).getTablesByNodeUnit();
            }
        }

        class ShowTableByNodeUnitHandler extends GetNodeTablesHandler {
            private final Map<String, BaseTableConfig> localTables;
            private final Set<String> tables = new LinkedHashSet<>();
            private final Set<String> views = new HashSet<>();

            ShowTableByNodeUnitHandler(String node, SchemaConfig config) {
                super(node, !config.isNoSharding());
                this.localTables = config.getTables();
            }

            @Override
            protected void handleTable(String table, String tableType) {
                if (tableType.equalsIgnoreCase("view"))
                    views.add(table);
                else if (!localTables.containsKey(table))
                    tables.add(table);
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

                if (!views.isEmpty()) {
                    tables.addAll(views);
                }
                return tables;
            }
        }
    }

    // show create table
    class ShowCreateTableHandler {
        private final DefaultNodeTableHandler parentHandler;

        ShowCreateTableHandler(DefaultNodeTableHandler parentHandler) {
            this.parentHandler = parentHandler;
        }

        private void execute(Set<String> tables) {
            logger.infoList("try to execute show create tables in [" + schema + "] default multi shardingNode:", getShardDNSet());
            for (String sharingNode : schemaConfig.getDefaultShardingNodes()) {
                new ShowCreateTableByNodeUnitHandler(this, schema, logger.isReload()).
                        execute(sharingNode, tables);
            }
        }

        private void countdown(String shardingNode, Set<String> remainingTables) {
            parentHandler.countdown(shardingNode, remainingTables);
        }

        private void handleTable(String shardingNode, String table, boolean isView, String sql) {
            parentHandler.handleTable(shardingNode, table, isView, sql);
        }

        class ShowCreateTableByNodeUnitHandler extends GetTableMetaHandler {
            private ShowCreateTableHandler parentHandler;

            ShowCreateTableByNodeUnitHandler(ShowCreateTableHandler parentHandler, String schema, boolean isReload) {
                super(schema, isReload);
                this.parentHandler = parentHandler;
            }

            @Override
            public void execute(String shardingNode, Set<String> tables) {
                super.execute(shardingNode, tables);
            }

            @Override
            protected void countdown(String shardingNode, Set<String> tables) {
                this.parentHandler.countdown(shardingNode, tables);
            }

            @Override
            protected void handleTable(String shardingNode, String table, boolean isView, String sql) {
                this.parentHandler.handleTable(shardingNode, table, isView, sql);
            }
        }
    }
}
