/*
 * Copyright (C) 2016-2018 ActionTech.
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
import com.actiontech.dble.meta.protocol.StructureMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MultiTablesMetaHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiTablesMetaHandler.class);
    private AtomicInteger shardTableCnt;
    private AtomicInteger singleTableCnt;
    private AtomicBoolean countDownFlag = new AtomicBoolean(false);
    private String schema;
    private SchemaConfig schemaConfig;
    private SchemaMetaHandler schemaMetaHandler;
    private Set<String> selfNode;
    private Lock singleTableLock = new ReentrantLock();
    private Condition collectTables = singleTableLock.newCondition();
    private Map<String, Map<String, List<String>>> tablesStructMap = new HashMap<>();

    public MultiTablesMetaHandler(SchemaMetaHandler schemaMetaHandler, SchemaConfig schemaConfig, Set<String> selfNode) {
        this.schemaMetaHandler = schemaMetaHandler;
        this.schemaConfig = schemaConfig;
        this.schema = schemaConfig.getName();
        this.selfNode = selfNode;
        this.singleTableCnt = new AtomicInteger(0);
    }

    public void execute() {
        this.schemaMetaHandler.getTmManager().createDatabase(schema);
        boolean existTable = false;
        if (schemaConfig.getDataNode() != null && (selfNode == null || !selfNode.contains(schemaConfig.getDataNode()))) {
            List<String> tables = getSingleTables();
            if (tables.size() > 0) {
                existTable = true;
                singleTableCnt.set(1);
                SingleNodeTablesMetaInitHandler tableHandler = new SingleNodeTablesMetaInitHandler(this, schema, tables, schemaConfig.getDataNode());
                tableHandler.execute();
            }
        }
        Map<String, Set<String>> dataNodeMap = new HashMap<>();
        for (Entry<String, TableConfig> entry : schemaConfig.getTables().entrySet()) {
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
        this.shardTableCnt = new AtomicInteger(dataNodeMap.size());

        AbstractTablesMetaHandler tableHandler = new TablesMetaInitHandler(this, schema, dataNodeMap, selfNode);
        tableHandler.execute();
        if (!existTable) {
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
            LOGGER.info("getSingleTables " + e);
            return new ArrayList<>();
        } finally {
            singleTableLock.unlock();
        }
        return showTablesHandler.getTables();
    }

    void showTablesFinished() {
        singleTableLock.lock();
        try {
            collectTables.signal();
        } finally {
            singleTableLock.unlock();
        }
    }

    void countDownSingleTable() {
        if (singleTableCnt.decrementAndGet() == 0) {
            countDown();
        }
    }

    void countDownShardTable() {
        if (shardTableCnt.decrementAndGet() == 0) {
            long version = System.currentTimeMillis();
            for (Map.Entry<String, Map<String, List<String>>> tablesStruct : tablesStructMap.entrySet()) {
                String tableName = tablesStruct.getKey();
                Map<String, List<String>> tableStruct = tablesStruct.getValue();
                StructureMeta.TableMeta tableMeta = null;
                if (tableStruct.size() > 1) {
                    // Through the SQL is different, the table Structure may still same.
                    // for example: autoIncrement number
                    Set<StructureMeta.TableMeta> tableMetas = new HashSet<>();
                    for (String sql : tableStruct.keySet()) {
                        tableMeta = MetaHelper.initTableMeta(tableName, sql, version);
                        tableMetas.add(tableMeta);
                    }
                    String tableId = schema + "." + tableName;
                    if (tableMetas.size() > 1) {
                        consistentWarning(tableName, tableStruct);
                    } else if (ToResolveContainer.TABLE_NOT_CONSISTENT_IN_DATAHOSTS.contains(tableId) &&
                            AlertUtil.alertSelfResolve(AlarmCode.TABLE_NOT_CONSISTENT_IN_DATAHOSTS, Alert.AlertLevel.WARN, AlertUtil.genSingleLabel("TABLE", tableId))) {
                        ToResolveContainer.TABLE_NOT_CONSISTENT_IN_DATAHOSTS.remove(tableId);
                    }
                    tableMetas.clear();
                } else if (tableStruct.size() == 1) {
                    String tableId = schema + "." + tableName;
                    if (ToResolveContainer.TABLE_NOT_CONSISTENT_IN_DATAHOSTS.contains(tableId) &&
                            AlertUtil.alertSelfResolve(AlarmCode.TABLE_NOT_CONSISTENT_IN_DATAHOSTS, Alert.AlertLevel.WARN, AlertUtil.genSingleLabel("TABLE", tableId))) {
                        ToResolveContainer.TABLE_NOT_CONSISTENT_IN_DATAHOSTS.remove(tableId);
                    }

                    String tableDetailId = "DataNode[" + tableStruct.values().iterator().next() + "]:Table[" + tableName + "]";
                    if (ToResolveContainer.TABLE_LACK.contains(tableId) && AlertUtil.alertSelfResolve(AlarmCode.TABLE_LACK, Alert.AlertLevel.WARN, AlertUtil.genSingleLabel("TABLE", tableDetailId))) {
                        ToResolveContainer.TABLE_LACK.remove(tableId);
                    }
                    tableMeta = MetaHelper.initTableMeta(tableName, tableStruct.keySet().iterator().next(), version);
                }

                if (tableMeta != null) {
                    this.getTmManager().addTable(schema, tableMeta);
                }
            }
            countDown();
        }
    }

    private void countDown() {
        if (shardTableCnt.get() == 0 && singleTableCnt.get() == 0) {
            if (countDownFlag.compareAndSet(false, true)) {
                schemaMetaHandler.countDown();
            }
        }
    }

    public ProxyMetaManager getTmManager() {
        return this.schemaMetaHandler.getTmManager();
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

    private void consistentWarning(String tableName, Map<String, List<String>> tableStruct) {
        String errorMsg = "Table [" + tableName + "] structure are not consistent in different data node!";
        LOGGER.warn(errorMsg);
        AlertUtil.alertSelf(AlarmCode.TABLE_NOT_CONSISTENT_IN_DATAHOSTS, Alert.AlertLevel.WARN, errorMsg, AlertUtil.genSingleLabel("TABLE", schema + "." + tableName));
        ToResolveContainer.TABLE_NOT_CONSISTENT_IN_DATAHOSTS.add(schema + "." + tableName);
        LOGGER.info("Currently detected: ");
        for (Map.Entry<String, List<String>> entry : tableStruct.entrySet()) {
            StringBuilder stringBuilder = new StringBuilder();
            for (String dn : entry.getValue()) {
                stringBuilder.append("DataNode:[").append(dn).append("]");
            }
            stringBuilder.append(":").append(entry);
            LOGGER.info(stringBuilder.toString());
        }
    }
}
