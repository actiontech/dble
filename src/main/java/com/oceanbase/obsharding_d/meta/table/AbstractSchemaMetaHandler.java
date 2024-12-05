/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.meta.table;

import com.oceanbase.obsharding_d.alarm.AlarmCode;
import com.oceanbase.obsharding_d.alarm.Alert;
import com.oceanbase.obsharding_d.alarm.AlertUtil;
import com.oceanbase.obsharding_d.alarm.ToResolveContainer;
import com.oceanbase.obsharding_d.config.model.sharding.SchemaConfig;
import com.oceanbase.obsharding_d.meta.ProxyMetaManager;
import com.oceanbase.obsharding_d.meta.ReloadLogHelper;
import com.oceanbase.obsharding_d.meta.TableMeta;
import com.oceanbase.obsharding_d.meta.ViewMeta;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * abstract impl of the single-sharding-MetaHandler
 * extends by SchemaCheckMetaHandler & SchemaInitMetaHandler
 */
public abstract class AbstractSchemaMetaHandler {
    protected final ReloadLogHelper logger;
    private String schema;
    private SchemaConfig schemaConfig;
    private Set<String> selfNode;
    private Set<String> filterTables;
    private final ProxyMetaManager tmManager;
    private AtomicBoolean countDownFlag = new AtomicBoolean(false);
    private List<ModeTableHandler> handlers = new ArrayList<>(3);

    AbstractSchemaMetaHandler(ProxyMetaManager tmManager, SchemaConfig schemaConfig, Set<String> selfNode, boolean isReload) {
        this.tmManager = tmManager;
        this.schemaConfig = schemaConfig;
        this.schema = schemaConfig.getName();
        this.selfNode = selfNode;
        this.logger = new ReloadLogHelper(isReload);
    }

    public void execute() {
        // default node
        handlers.add(new DefaultNodeTableHandler(this));
        // fake tables config
        handlers.add(new FakeConfigTableHandler(this, getTmManager()));
        // tables config
        handlers.add(new ConfigTableHandler(this));

        boolean existTable = false;
        for (ModeTableHandler handler : handlers) {
            if (handler.loadMetaData())
                existTable = true;
        }
        if (!existTable) {
            logger.info("no table exist in schema " + schema + ",count down");
            tryMetadataComplete();
        }
    }

    public void checkTableConsistent(Map<String, Map<String, List<String>>> tablesStructMap, String table, String shardingNode, String sql) {
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

    void tryMetadataComplete() {
        boolean isAllComplete = true;
        for (ModeTableHandler handler : handlers) {
            if (!handler.isComplete()) {
                isAllComplete = false;
            }
        }
        if (isAllComplete) {
            if (countDownFlag.compareAndSet(false, true)) {
                logger.info("schema[" + schema + "] loading metadata is completed");
                schemaMetaFinish();
            }
        }
    }

    void tryToAddMetadata(Map<String, Map<String, List<String>>> tablesStructMap) {
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
                    logger.info("Table [" + tableName + "] structure of all shardingNodes has been restored to be consistent!");
                    AlertUtil.alertSelfResolve(AlarmCode.TABLE_NOT_CONSISTENT_IN_SHARDINGS, Alert.AlertLevel.WARN, AlertUtil.genSingleLabel("TABLE", tableId),
                            ToResolveContainer.TABLE_NOT_CONSISTENT_IN_SHARDINGS, tableId);
                }
                handleMultiMetaData(tableMetas);
                tableMetas.clear();
            } else if (tableStruct.size() == 1 && tableStruct.keySet().iterator().next() != null) {
                String tableId = schema + "." + tableName;
                if (ToResolveContainer.TABLE_NOT_CONSISTENT_IN_SHARDINGS.contains(tableId)) {
                    logger.info("Table [" + tableName + "] structure of all shardingNodes has been restored to be consistent!");
                    AlertUtil.alertSelfResolve(AlarmCode.TABLE_NOT_CONSISTENT_IN_SHARDINGS, Alert.AlertLevel.WARN, AlertUtil.genSingleLabel("TABLE", tableId),
                            ToResolveContainer.TABLE_NOT_CONSISTENT_IN_SHARDINGS, tableId);
                }
                List<String> shardingNodes = tableStruct.values().iterator().next();
                for (String shardingNodei : shardingNodes) {
                    String tableLackKey = AlertUtil.getTableLackKey(shardingNodei, tableName);
                    if (ToResolveContainer.TABLE_LACK.contains(tableLackKey)) {
                        AlertUtil.alertSelfResolve(AlarmCode.TABLE_LACK, Alert.AlertLevel.WARN, AlertUtil.genSingleLabel("TABLE", tableLackKey),
                                ToResolveContainer.TABLE_LACK, tableLackKey);
                    }
                }
                TableMeta tableMeta = MetaHelper.initTableMeta(tableName, tableStruct.keySet().iterator().next(), version, schema);
                handleSingleMetaData(tableMeta);
            }
        }
        tryMetadataComplete();
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


    public ProxyMetaManager getTmManager() {
        return tmManager;
    }

    public String getSchema() {
        return schema;
    }

    public SchemaConfig getSchemaConfig() {
        return schemaConfig;
    }

    public Set<String> getSelfNode() {
        return selfNode;
    }

    public Set<String> getFilterTables() {
        return filterTables;
    }

    public ReloadLogHelper getLogger() {
        return logger;
    }


    abstract void handleSingleMetaData(TableMeta tableMeta);

    abstract void handleViewMeta(ViewMeta viewMeta);

    abstract void handleMultiMetaData(Set<TableMeta> tableMetas);

    abstract void schemaMetaFinish();

}
