package com.actiontech.dble.meta.table;

import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.sharding.table.BaseTableConfig;
import com.actiontech.dble.meta.ReloadLogHelper;
import com.actiontech.dble.meta.TableMeta;
import com.actiontech.dble.meta.ViewMeta;
import com.actiontech.dble.util.CollectionUtil;
import com.google.common.collect.Sets;

import java.util.*;

public class DefaultNodeTableHandler extends ModeTableHandler {
    private final AbstractSchemaMetaHandler operationalHandler;
    private final SchemaConfig schemaConfig;
    private final Set<String> selfNodes;
    private final ReloadLogHelper logger;
    private final Set<String> filterTables;

    public DefaultNodeTableHandler(AbstractSchemaMetaHandler operationalHandler) {
        this.operationalHandler = operationalHandler;
        this.schemaConfig = operationalHandler.getSchemaConfig();
        this.selfNodes = operationalHandler.getSelfNode();
        this.logger = operationalHandler.getLogger();
        this.filterTables = operationalHandler.getFilterTables();
    }

    @Override
    public boolean loadMetaData() {
        boolean existTable = false;
        if (!CollectionUtil.isEmpty(schemaConfig.getDefaultShardingNodes()) &&
                (selfNodes == null || Collections.disjoint(selfNodes, schemaConfig.getDefaultShardingNodes()))) {
            Set<String> showTablesResult = new ShowTableHandler(schemaConfig).tryGetTables();
            if (!CollectionUtil.isEmpty(filterTables) && schemaConfig.isDefaultSingleNode()) {
                showTablesResult.retainAll(filterTables);
                filterTables.removeAll(showTablesResult);
            }

            if (showTablesResult.size() > 0) {
                existTable = true;
                new ShowCreateTableHandler(operationalHandler, schemaConfig, logger, new HashSet<>(schemaConfig.getDefaultShardingNodes())).execute(showTablesResult);
            }
        }
        return existTable;
    }

    // show table
    static class ShowTableHandler {
        private final SchemaConfig schemaConfig;
        private List<ShowTableByNodeUnitHandler> nodeUnitHandlers;

        ShowTableHandler(SchemaConfig schemaConfig) {
            this.schemaConfig = schemaConfig;
        }

        private Set<String> tryGetTables() {
            List<String> nodes;
            if ((nodes = schemaConfig.getDefaultShardingNodes()) == null) return Sets.newHashSet();
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

        static class ShowTableByNodeUnitHandler extends GetNodeTablesHandler {
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
    static class ShowCreateTableHandler {
        private final AbstractSchemaMetaHandler operationalHandler;
        private final String schema;
        private final SchemaConfig schemaConfig;
        private volatile Map<String, Map<String, List<String>>> tablesStructMap = new HashMap<>();
        private final ReloadLogHelper logger;
        private volatile Set<String> shardDNSet;

        ShowCreateTableHandler(AbstractSchemaMetaHandler operationalHandler, SchemaConfig schemaConfig, ReloadLogHelper logger, Set<String> shardDNSet) {
            this.operationalHandler = operationalHandler;
            this.schema = schemaConfig.getName();
            this.schemaConfig = schemaConfig;
            this.shardDNSet = shardDNSet;
            this.logger = logger;
        }

        private void execute(Set<String> tables) {
            for (String sharingNode : schemaConfig.getDefaultShardingNodes()) {
                new ShowCreateTableByNodeUnitHandler(this, schema, logger.isReload()).
                        execute(sharingNode, tables);
            }
        }

        private synchronized void countdown(String shardingNode, Set<String> remainingTables) {
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

        public void tryComplete(String shardingNode, boolean isLastShardingNode) {
            if (isLastShardingNode) {
                logger.info("implicit defaultNode tables in schema[" + schema + "], last shardingNode[" + shardingNode + "] ");
                operationalHandler.tryToAddMetadata(tablesStructMap);
            }
        }

        private void handleTable(String shardingNode, String table, boolean isView, String createSQL) {
            if (schemaConfig.isDefaultSingleNode()) {
                if (isView) {
                    ViewMeta viewMeta = MetaHelper.initViewMeta(schema, createSQL, System.currentTimeMillis(), operationalHandler.getTmManager());
                    operationalHandler.handleViewMeta(viewMeta);
                } else {
                    TableMeta tableMeta = MetaHelper.initTableMeta(table, createSQL, System.currentTimeMillis(), schema);
                    operationalHandler.handleSingleMetaData(tableMeta);
                }
            } else {
                if (isView) return;
                operationalHandler.checkTableConsistent(tablesStructMap, table, shardingNode, createSQL);
            }
        }

        private boolean isLastShardingNode(String shardingNode) {
            shardDNSet.remove(shardingNode);
            return shardDNSet.size() == 0;
        }

        static class ShowCreateTableByNodeUnitHandler extends GetTableMetaHandler {
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
