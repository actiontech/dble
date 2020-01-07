/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.meta.table;

import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.alarm.ToResolveContainer;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.meta.ProxyMetaManager;
import com.actiontech.dble.meta.ReloadLogHelper;
import com.actiontech.dble.meta.ViewMeta;
import com.actiontech.dble.meta.protocol.StructureMeta;
import com.actiontech.dble.util.CollectionUtil;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * abstract impl of the single-schema-MetaHandler
 * extends by SchemaCheckMetaHandler & SchemaInitMetaHandler
 */
public abstract class AbstractSchemaMetaHandler {
    protected final ReloadLogHelper logger;
    //shard-DataNode-Set when the set to be count down into the empty,means that all the dataNode have finished
    private volatile Set<String> shardDNSet = new HashSet<>();
    //defaultDNflag  has no default Node/default Node has no table/ default Node table return finish ---- false
    // schema has default Node & default Node has not return yet  ---- true
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
            logger.info("single dataNode countdown[" + schema + "]");
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
        String defaultNode = schemaConfig.getDataNode();
        if (defaultNode != null && (selfNode == null || !selfNode.contains(defaultNode))) {
            Set<String> tables = getTablesFromDefaultDataNode();
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
        Map<String, Set<String>> dataNodeMap = new HashMap<>();
        for (Map.Entry<String, TableConfig> entry : filterConfigTables().entrySet()) {
            existTable = true;
            String tableName = entry.getKey();
            TableConfig tbConfig = entry.getValue();
            for (String dataNode : tbConfig.getDataNodes()) {
                Set<String> tables = dataNodeMap.get(dataNode);
                if (tables == null) {
                    tables = new HashSet<>();
                    dataNodeMap.put(dataNode, tables);
                    shardDNSet.add(dataNode);
                }
                tables.add(tableName);
            }
        }

        logger.infoList("try to execute show create table in [" + schema + "] dataNodes:", shardDNSet);
        ConfigTableMetaHandler tableHandler = new ConfigTableMetaHandler(this, schema, selfNode, logger.isReload());
        tableHandler.execute(dataNodeMap);
        if (!existTable) {
            logger.info("no table exist in schema " + schema + ",count down");
            countDown();
        }
    }

    private Set<String> getTablesFromDefaultDataNode() {
        GetDefaultNodeTablesHandler showTablesHandler = new GetDefaultNodeTablesHandler(schemaConfig);
        showTablesHandler.execute();
        return showTablesHandler.getTables();
    }

    private Map<String, TableConfig> filterConfigTables() {
        Map<String, TableConfig> newReload = new HashMap<>();
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

    public synchronized void checkTableConsistent(String table, String dataNode, String sql) {
        Map<String, List<String>> tableStruct = tablesStructMap.get(table);
        if (tableStruct == null) {
            tableStruct = new HashMap<>();
            List<String> dataNodeList = new LinkedList<>();
            dataNodeList.add(dataNode);
            tableStruct.put(sql, dataNodeList);
            tablesStructMap.put(table, tableStruct);
        } else if (tableStruct.containsKey(sql)) {
            List<String> dataNodeList = tableStruct.get(sql);
            dataNodeList.add(dataNode);
        } else {
            List<String> dataNodeList = new LinkedList<>();
            dataNodeList.add(dataNode);
            tableStruct.put(sql, dataNodeList);
        }
    }

    private synchronized boolean countDownShardDN(String dataNode) {
        shardDNSet.remove(dataNode);
        return shardDNSet.size() == 0;
    }


    void countDownShardTable(String dataNode) {
        logger.info("shard dataNode count down[" + schema + "][" + dataNode + "] ");
        if (countDownShardDN(dataNode)) {
            long version = System.currentTimeMillis();
            for (Map.Entry<String, Map<String, List<String>>> tablesStruct : tablesStructMap.entrySet()) {
                String tableName = tablesStruct.getKey();
                Map<String, List<String>> tableStruct = tablesStruct.getValue();
                if (tableStruct.size() > 1) {
                    // Through the SQL is different, the table Structure may still same.
                    // for example: autoIncrement number
                    Set<StructureMeta.TableMeta> tableMetas = new HashSet<>();
                    for (String sql : tableStruct.keySet()) {
                        StructureMeta.TableMeta tableMeta = MetaHelper.initTableMeta(tableName, sql, version);
                        tableMetas.add(tableMeta);
                    }
                    String tableId = schema + "." + tableName;
                    if (tableMetas.size() > 1) {
                        consistentWarning(tableName, tableStruct);
                    } else if (ToResolveContainer.TABLE_NOT_CONSISTENT_IN_DATAHOSTS.contains(tableId)) {
                        AlertUtil.alertSelfResolve(AlarmCode.TABLE_NOT_CONSISTENT_IN_DATAHOSTS, Alert.AlertLevel.WARN, AlertUtil.genSingleLabel("TABLE", tableId),
                                ToResolveContainer.TABLE_NOT_CONSISTENT_IN_DATAHOSTS, tableId);
                    }
                    handleMultiMetaData(tableMetas);
                    tableMetas.clear();
                } else if (tableStruct.size() == 1) {
                    String tableId = schema + "." + tableName;
                    if (ToResolveContainer.TABLE_NOT_CONSISTENT_IN_DATAHOSTS.contains(tableId)) {
                        AlertUtil.alertSelfResolve(AlarmCode.TABLE_NOT_CONSISTENT_IN_DATAHOSTS, Alert.AlertLevel.WARN, AlertUtil.genSingleLabel("TABLE", tableId),
                                ToResolveContainer.TABLE_NOT_CONSISTENT_IN_DATAHOSTS, tableId);
                    }

                    String tableDetailId = "DataNode[" + tableStruct.values().iterator().next() + "]:Table[" + tableName + "]";
                    if (ToResolveContainer.TABLE_LACK.contains(tableId)) {
                        AlertUtil.alertSelfResolve(AlarmCode.TABLE_LACK, Alert.AlertLevel.WARN, AlertUtil.genSingleLabel("TABLE", tableDetailId),
                                ToResolveContainer.TABLE_LACK, tableId);
                    }
                    StructureMeta.TableMeta tableMeta = MetaHelper.initTableMeta(tableName, tableStruct.keySet().iterator().next(), version);
                    handleSingleMetaData(tableMeta);
                }
            }
            logger.info("shard dataNode finish countdown to schema [" + schema + "]");
            countDown();
        }

    }

    private synchronized void consistentWarning(String tableName, Map<String, List<String>> tableStruct) {
        String errorMsg = "Table [" + tableName + "] structure are not consistent in different data node!";
        logger.warn(errorMsg);
        AlertUtil.alertSelf(AlarmCode.TABLE_NOT_CONSISTENT_IN_DATAHOSTS, Alert.AlertLevel.WARN, errorMsg, AlertUtil.genSingleLabel("TABLE", schema + "." + tableName));
        ToResolveContainer.TABLE_NOT_CONSISTENT_IN_DATAHOSTS.add(schema + "." + tableName);
        logger.info("Currently detected: ");
        for (Map.Entry<String, List<String>> entry : tableStruct.entrySet()) {
            StringBuilder stringBuilder = new StringBuilder("{");
            for (String dn : entry.getValue()) {
                stringBuilder.append("DataNode:[").append(dn).append("]");
            }
            stringBuilder.append("}_Struct:").append(entry.getKey());
            logger.info(stringBuilder.toString());
        }
    }

    public void setFilterTables(Set<String> filterTables) {
        this.filterTables = filterTables;
    }

    abstract void handleSingleMetaData(StructureMeta.TableMeta tableMeta);

    abstract void handleViewMeta(ViewMeta viewMeta);

    abstract void handleMultiMetaData(Set<StructureMeta.TableMeta> tableMetas);

    abstract void schemaMetaFinish();


}
