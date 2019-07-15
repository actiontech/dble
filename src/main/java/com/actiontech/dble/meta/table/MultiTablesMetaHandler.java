/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.meta.table;

import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.alarm.ToResolveContainer;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.meta.ReloadLogUtil;
import com.actiontech.dble.meta.protocol.StructureMeta;
import com.actiontech.dble.util.CollectionUtil;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public abstract class MultiTablesMetaHandler {
    protected final ReloadLogUtil logger;
    private AtomicInteger shardTableDNCnt;
    private AtomicInteger singleTableDNCnt;
    private AtomicBoolean countDownFlag = new AtomicBoolean(false);
    private String schema;
    private SchemaConfig schemaConfig;
    private Set<String> selfNode;
    private Lock singleTableLock = new ReentrantLock();
    private Condition collectTables = singleTableLock.newCondition();
    private Map<String, Map<String, List<String>>> tablesStructMap = new HashMap<>();
    private Set<String> filterTables;


    MultiTablesMetaHandler(SchemaConfig schemaConfig, Set<String> selfNode, boolean isReload) {
        this.schemaConfig = schemaConfig;
        this.schema = schemaConfig.getName();
        this.selfNode = selfNode;
        this.singleTableDNCnt = new AtomicInteger(0);
        logger = new ReloadLogUtil(isReload);
    }


    void countDownSingleTable() {
        if (singleTableDNCnt.decrementAndGet() == 0) {
            logger.info("single dataNode countdown[" + schema + "]");
            countDown();
        }
    }

    protected void countDown() {
        if (shardTableDNCnt.get() == 0 && singleTableDNCnt.get() == 0) {
            logger.info("all shardTableDNCnt&singleTableDNCnt count down[" + schema + "]");
            if (countDownFlag.compareAndSet(false, true)) {
                schemaMetaFinish();
            }
        }
    }


    public void execute() {
        boolean existTable = false;
        if (schemaConfig.getDataNode() != null && (selfNode == null || !selfNode.contains(schemaConfig.getDataNode()))) {
            List<String> tables = getSingleTables();
            if (!CollectionUtil.isEmpty(filterTables)) {
                tables.retainAll(filterTables);
                filterTables.removeAll(tables);
            }
            if (tables.size() > 0) {
                existTable = true;
                singleTableDNCnt.set(1);
                SingleNodeTablesMetaInitHandler tableHandler = new SingleNodeTablesMetaInitHandler(this, tables, schemaConfig.getDataNode(), logger.isReload());
                tableHandler.execute();
            }
        }
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
                }
                tables.add(tableName);
            }
        }
        this.shardTableDNCnt = new AtomicInteger(dataNodeMap.size());

        logger.infoList("try to execute show create table in dataNode", dataNodeMap.keySet());
        AbstractTablesMetaHandler tableHandler = new TablesMetaInitHandler(this, schema, dataNodeMap, selfNode, logger.isReload());
        tableHandler.execute();
        if (!existTable) {
            logger.info("no table exist in schema " + schema + ",count down");
            countDown();
        }
    }

    private List<String> getSingleTables() {
        GetSchemaDefaultNodeTablesHandler showTablesHandler = new GetSchemaDefaultNodeTablesHandler(this, schemaConfig);
        showTablesHandler.execute();
        singleTableLock.lock();
        try {
            while (!showTablesHandler.isFinished()) {
                collectTables.await();
            }
        } catch (InterruptedException e) {
            logger.info("getSingleTables " + e);
            return new ArrayList<>();
        } finally {
            singleTableLock.unlock();
        }
        return showTablesHandler.getTables();
    }


    void showTablesFinished() {
        singleTableLock.lock();
        try {
            collectTables.signalAll();
        } finally {
            singleTableLock.unlock();
        }
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


    void countDownShardTable() {
        logger.info("shard dataNode count down[" + schema + "] Count reming is " + shardTableDNCnt.get());
        if (shardTableDNCnt.decrementAndGet() == 0) {
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

    abstract void handleMultiMetaData(Set<StructureMeta.TableMeta> tableMetas);

    abstract void schemaMetaFinish();


}
