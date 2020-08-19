/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.meta.table;

import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.alarm.ToResolveContainer;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.sharding.table.BaseTableConfig;
import com.actiontech.dble.meta.ProxyMetaManager;
import com.actiontech.dble.meta.ReloadLogHelper;
import com.actiontech.dble.meta.TableMeta;
import com.actiontech.dble.meta.ViewMeta;
import com.actiontech.dble.util.CollectionUtil;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * abstract impl of the single-sharding-MetaHandler
 * extends by SchemaCheckMetaHandler & SchemaInitMetaHandler
 */
public abstract class AbstractSchemaMetaHandler {
    protected final ReloadLogHelper logger;
    //shard-ShardingNode-Set when the set to be count down into the empty,means that all the shardingNode have finished
    private volatile Set<String> shardDNSet = new HashSet<>();
    //defaultDNflag  has no default Node/default Node has no table/ default Node table return finish ---- false
    // sharding has default Node & default Node has not return yet  ---- true
    private AtomicBoolean defaultDNflag;
    private AtomicBoolean countDownFlag = new AtomicBoolean(false);
    private String schema;
    private SchemaConfig schemaConfig;
    private Set<String> selfNode;
    private Map<String, Map<String, List<String>>> tablesStructMap = new HashMap<>();
    private Set<String> filterTables;
    private final ProxyMetaManager tmManager;

    AbstractSchemaMetaHandler(ProxyMetaManager tmManager, SchemaConfig schemaConfig, Set<String> selfNode, boolean isReload) {
        this.tmManager = tmManager;
        this.schemaConfig = schemaConfig;
        this.schema = schemaConfig.getName();
        this.selfNode = selfNode;
        this.logger = new ReloadLogHelper(isReload);
        this.defaultDNflag = new AtomicBoolean(false);
    }

    public ProxyMetaManager getTmManager() {
        return tmManager;
    }

    void countDownSingleTable() {
        if (defaultDNflag.compareAndSet(true, false)) {
            logger.info("single shardingNode countdown[" + schema + "]");
            countDown();
        }
    }

    protected synchronized void countDown() {
        if (shardDNSet.isEmpty() && !defaultDNflag.get()) {
            logger.info("all shardDNSet&defaultDNflag count down[" + schema + "]");
            if (countDownFlag.compareAndSet(false, true)) {
                schemaMetaFinish();
            }
        }
    }

    public void execute() {
        boolean existTable = false;
        // default node
        String defaultNode = schemaConfig.getShardingNode();
        if (defaultNode != null && (selfNode == null || !selfNode.contains(defaultNode))) {
            Set<String> tables = getTablesFromDefaultShardingNode();
            if (!CollectionUtil.isEmpty(filterTables)) {
                tables.retainAll(filterTables);
                filterTables.removeAll(tables);
            }
            if (tables.size() > 0) {
                existTable = true;
                defaultDNflag.set(true);
                DefaultNodeTablesMetaHandler tableHandler = new DefaultNodeTablesMetaHandler(this, schema, logger.isReload());
                tableHandler.execute(defaultNode, tables);
            }
        }

        // tables in config
        Map<String, Set<String>> shardingNodeMap = new HashMap<>();
        for (Map.Entry<String, BaseTableConfig> entry : filterConfigTables().entrySet()) {
            existTable = true;
            String tableName = entry.getKey();
            BaseTableConfig tbConfig = entry.getValue();
            for (String shardingNode : tbConfig.getShardingNodes()) {
                Set<String> tables = shardingNodeMap.get(shardingNode);
                if (tables == null) {
                    tables = new HashSet<>();
                    shardingNodeMap.put(shardingNode, tables);
                    shardDNSet.add(shardingNode);
                }
                tables.add(tableName);
            }
        }

        logger.infoList("try to execute show create table in [" + schema + "] shardingNode:", shardDNSet);
        ConfigTableMetaHandler tableHandler = new ConfigTableMetaHandler(this, schema, selfNode, logger.isReload());
        tableHandler.execute(shardingNodeMap);
        if (!existTable) {
            logger.info("no table exist in schema " + schema + ",count down");
            countDown();
        }
    }

    private Set<String> getTablesFromDefaultShardingNode() {
        GetDefaultNodeTablesHandler showTablesHandler = new GetDefaultNodeTablesHandler(schemaConfig);
        showTablesHandler.execute();
        return showTablesHandler.getTables();
    }

    private Map<String, BaseTableConfig> filterConfigTables() {
        Map<String, BaseTableConfig> newReload = new HashMap<>();
        if (filterTables == null) {
            newReload = schemaConfig.getTables();
        } else {
            for (String table : filterTables) {
                if (schemaConfig.getTables().containsKey(table)) {
                    newReload.put(table, schemaConfig.getTables().get(table));
                } else {
                    logger.warn("reload table[" + schema + "." + table + "] metadata, but table doesn't exist");
                }
            }
        }
        return newReload;
    }

    public synchronized void checkTableConsistent(String table, String shardingNode, String sql) {
        Map<String, List<String>> tableStruct = tablesStructMap.get(table);
        if (tableStruct == null) {
            tableStruct = new HashMap<>();
            List<String> shardingNodeList = new LinkedList<>();
            shardingNodeList.add(shardingNode);
            tableStruct.put(sql, shardingNodeList);
            tablesStructMap.put(table, tableStruct);
        } else if (tableStruct.containsKey(sql)) {
            List<String> shardingNodeList = tableStruct.get(sql);
            shardingNodeList.add(shardingNode);
        } else {
            List<String> shardingNodeList = new LinkedList<>();
            shardingNodeList.add(shardingNode);
            tableStruct.put(sql, shardingNodeList);
        }
    }

    private synchronized boolean countDownShardDN(String shardingNode) {
        shardDNSet.remove(shardingNode);
        return shardDNSet.size() == 0;
    }


    void countDownShardTable(String shardingNode) {
        logger.info("shardingNode count down[" + schema + "][" + shardingNode + "] ");
        if (countDownShardDN(shardingNode)) {
            long version = System.currentTimeMillis();
            for (Map.Entry<String, Map<String, List<String>>> tablesStruct : tablesStructMap.entrySet()) {
                String tableName = tablesStruct.getKey();
                Map<String, List<String>> tableStruct = tablesStruct.getValue();
                if (tableStruct.size() > 1) {
                    // Through the SQL is different, the table Structure may still same.
                    // for example: autoIncrement number
                    Set<TableMeta> tableMetas = new HashSet<>();
                    for (String sql : tableStruct.keySet()) {
                        TableMeta tableMeta = MetaHelper.initTableMeta(tableName, sql, version, schema);
                        tableMetas.add(tableMeta);
                    }
                    String tableId = schema + "." + tableName;
                    if (tableMetas.size() > 1) {
                        consistentWarning(tableName, tableStruct);
                    } else if (ToResolveContainer.TABLE_NOT_CONSISTENT_IN_SHARDINGS.contains(tableId)) {
                        AlertUtil.alertSelfResolve(AlarmCode.TABLE_NOT_CONSISTENT_IN_SHARDINGS, Alert.AlertLevel.WARN, AlertUtil.genSingleLabel("TABLE", tableId),
                                ToResolveContainer.TABLE_NOT_CONSISTENT_IN_SHARDINGS, tableId);
                    }
                    handleMultiMetaData(tableMetas);
                    tableMetas.clear();
                } else if (tableStruct.size() == 1) {
                    String tableId = schema + "." + tableName;
                    if (ToResolveContainer.TABLE_NOT_CONSISTENT_IN_SHARDINGS.contains(tableId)) {
                        AlertUtil.alertSelfResolve(AlarmCode.TABLE_NOT_CONSISTENT_IN_SHARDINGS, Alert.AlertLevel.WARN, AlertUtil.genSingleLabel("TABLE", tableId),
                                ToResolveContainer.TABLE_NOT_CONSISTENT_IN_SHARDINGS, tableId);
                    }

                    String tableDetailId = "sharding_node[" + tableStruct.values().iterator().next() + "]:Table[" + tableName + "]";
                    if (ToResolveContainer.TABLE_LACK.contains(tableId)) {
                        AlertUtil.alertSelfResolve(AlarmCode.TABLE_LACK, Alert.AlertLevel.WARN, AlertUtil.genSingleLabel("TABLE", tableDetailId),
                                ToResolveContainer.TABLE_LACK, tableId);
                    }
                    TableMeta tableMeta = MetaHelper.initTableMeta(tableName, tableStruct.keySet().iterator().next(), version, schema);
                    handleSingleMetaData(tableMeta);
                }
            }
            logger.info("shardingNode finish countdown to schema [" + schema + "]");
            countDown();
        }

    }

    private synchronized void consistentWarning(String tableName, Map<String, List<String>> tableStruct) {
        String errorMsg = "Table [" + tableName + "] structure are not consistent in different shardingNode!";
        logger.warn(errorMsg);
        AlertUtil.alertSelf(AlarmCode.TABLE_NOT_CONSISTENT_IN_SHARDINGS, Alert.AlertLevel.WARN, errorMsg, AlertUtil.genSingleLabel("TABLE", schema + "." + tableName));
        ToResolveContainer.TABLE_NOT_CONSISTENT_IN_SHARDINGS.add(schema + "." + tableName);
        logger.info("Currently detected: ");
        for (Map.Entry<String, List<String>> entry : tableStruct.entrySet()) {
            StringBuilder stringBuilder = new StringBuilder("{");
            for (String dn : entry.getValue()) {
                stringBuilder.append("shardingNode:[").append(dn).append("]");
            }
            stringBuilder.append("}_Struct:").append(entry.getKey());
            logger.info(stringBuilder.toString());
        }
    }

    public void setFilterTables(Set<String> filterTables) {
        this.filterTables = filterTables;
    }

    abstract void handleSingleMetaData(TableMeta tableMeta);

    abstract void handleViewMeta(ViewMeta viewMeta);

    abstract void handleMultiMetaData(Set<TableMeta> tableMetas);

    abstract void schemaMetaFinish();


}
