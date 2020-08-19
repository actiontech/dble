/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.meta.table;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.alarm.ToResolveContainer;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.backend.datasource.ShardingNode;
import com.actiontech.dble.meta.ReloadLogHelper;
import com.actiontech.dble.meta.TableMeta;
import com.actiontech.dble.sqlengine.OneRawSQLQueryResultHandler;
import com.actiontech.dble.sqlengine.SQLJob;
import com.actiontech.dble.sqlengine.SQLQueryResult;
import com.actiontech.dble.sqlengine.SQLQueryResultListener;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractTableMetaHandler {
    protected final ReloadLogHelper logger;
    private static final String[] MYSQL_SHOW_CREATE_TABLE_COLS = new String[]{
            "Table",
            "Create Table"};
    private static final String SQL_PREFIX = "show create table ";

    protected String tableName;
    private List<String> shardingNodes;
    private AtomicInteger nodesNumber;
    protected String schema;
    private Set<String> selfNode;
    private ConcurrentMap<String, List<String>> shardingNodeTableStructureSQLMap;


    public AbstractTableMetaHandler(String schema, String tableName, List<String> shardingNodes, Set<String> selfNode, boolean isReload) {
        this.shardingNodes = shardingNodes;
        this.nodesNumber = new AtomicInteger(shardingNodes.size());
        this.schema = schema;
        this.selfNode = selfNode;
        this.tableName = tableName;
        this.shardingNodeTableStructureSQLMap = new ConcurrentHashMap<>();
        this.logger = new ReloadLogHelper(isReload);
    }

    public void execute() {
        logger.info("table " + tableName + " execute start");
        for (String shardingNode : shardingNodes) {
            if (selfNode != null && selfNode.contains(shardingNode)) {
                this.countdown();
                return;
            }
            ShardingNode dn = DbleServer.getInstance().getConfig().getShardingNodes().get(shardingNode);
            PhysicalDbInstance ds = dn.getDbGroup().getWriteDbInstance();
            String sql = SQL_PREFIX + "`" + tableName + "`";
            if (ds.isAlive()) {
                OneRawSQLQueryResultHandler resultHandler = new OneRawSQLQueryResultHandler(MYSQL_SHOW_CREATE_TABLE_COLS, new MySQLTableStructureListener(shardingNode, System.currentTimeMillis(), ds));
                SQLJob sqlJob = new SQLJob(sql, dn.getDatabase(), resultHandler, ds);
                sqlJob.run();
            } else {
                OneRawSQLQueryResultHandler resultHandler = new OneRawSQLQueryResultHandler(MYSQL_SHOW_CREATE_TABLE_COLS, new MySQLTableStructureListener(shardingNode, System.currentTimeMillis(), null));
                SQLJob sqlJob = new SQLJob(sql, shardingNode, resultHandler, false);
                sqlJob.run();
            }
        }
    }

    protected abstract void countdown();

    protected abstract void handlerTable(TableMeta tableMeta);

    private class MySQLTableStructureListener implements SQLQueryResultListener<SQLQueryResult<Map<String, String>>> {
        private String shardingNode;
        private long version;
        private PhysicalDbInstance ds;

        MySQLTableStructureListener(String shardingNode, long version, PhysicalDbInstance ds) {
            this.shardingNode = shardingNode;
            this.version = version;
            this.ds = ds;
        }

        @Override
        public void onResult(SQLQueryResult<Map<String, String>> result) {
            String tableId = "sharding_node[" + shardingNode + "]:Table[" + tableName + "]";
            logger.info(tableId + " on result " + result.isSuccess() + " count is " + nodesNumber);
            String key = null;
            if (ds != null) {
                key = "dbInstance[" + ds.getDbGroupConfig().getName() + "." + ds.getConfig().getInstanceName() + "],sharding_node[" + shardingNode + "],schema[" + schema + "]";
            }
            if (!result.isSuccess()) {
                //not thread safe
                String warnMsg = "Can't get table " + tableName + "'s config from shardingNode:" + shardingNode + "! Maybe the table is not initialized!";
                logger.warn(warnMsg);
                AlertUtil.alertSelf(AlarmCode.TABLE_LACK, Alert.AlertLevel.WARN, warnMsg, AlertUtil.genSingleLabel("TABLE", tableId));
                ToResolveContainer.TABLE_LACK.add(tableId);
                if (nodesNumber.decrementAndGet() == 0) {
                    logger.info(tableId + " count down to 0 ,try to count down the table");
                    TableMeta tableMeta = genTableMeta();
                    handlerTable(tableMeta);
                    countdown();
                }
                return;
            } else {
                if (ToResolveContainer.TABLE_LACK.contains(tableId)) {
                    AlertUtil.alertSelfResolve(AlarmCode.TABLE_LACK, Alert.AlertLevel.WARN, AlertUtil.genSingleLabel("TABLE", tableId),
                            ToResolveContainer.TABLE_LACK, tableId);
                }
                if (ds != null && ToResolveContainer.SHARDING_NODE_LACK.contains(key)) {
                    Map<String, String> labels = AlertUtil.genSingleLabel("dbInstance", ds.getDbGroupConfig().getName() + "-" + ds.getConfig().getInstanceName());
                    labels.put("sharding_node", shardingNode);
                    AlertUtil.alertResolve(AlarmCode.SHARDING_NODE_LACK, Alert.AlertLevel.WARN, "mysql", ds.getConfig().getId(), labels,
                            ToResolveContainer.SHARDING_NODE_LACK, key);
                }
            }

            String currentSql = result.getResult().get(MYSQL_SHOW_CREATE_TABLE_COLS[1]);
            if (shardingNodeTableStructureSQLMap.containsKey(currentSql)) {
                List<String> shardingNodeList = shardingNodeTableStructureSQLMap.get(currentSql);
                shardingNodeList.add(shardingNode);
            } else {
                List<String> shardingNodeList = new LinkedList<>();
                shardingNodeList.add(shardingNode);
                shardingNodeTableStructureSQLMap.put(currentSql, shardingNodeList);
            }

            if (nodesNumber.decrementAndGet() == 0) {
                logger.info(tableId + " count down to 0 ,try to count down the table");
                TableMeta tableMeta = genTableMeta();
                handlerTable(tableMeta);
                countdown();
            }
        }

        private TableMeta genTableMeta() {
            TableMeta tableMeta = null;
            if (shardingNodeTableStructureSQLMap.size() > 1) {
                // Through the SQL is different, the table Structure may still same.
                // for example: autoIncrement number
                Set<TableMeta> tableMetas = new HashSet<>();
                for (String sql : shardingNodeTableStructureSQLMap.keySet()) {
                    tableMeta = MetaHelper.initTableMeta(tableName, sql, version, schema);
                    tableMetas.add(tableMeta);
                }
                String tableId = schema + "." + tableName;
                if (tableMetas.size() > 1) {
                    consistentWarning();
                } else if (ToResolveContainer.TABLE_NOT_CONSISTENT_IN_SHARDINGS.contains(tableId)) {
                    AlertUtil.alertSelfResolve(AlarmCode.TABLE_NOT_CONSISTENT_IN_SHARDINGS, Alert.AlertLevel.WARN, AlertUtil.genSingleLabel("TABLE", tableId),
                            ToResolveContainer.TABLE_NOT_CONSISTENT_IN_SHARDINGS, tableId);
                }
                tableMetas.clear();
            } else if (shardingNodeTableStructureSQLMap.size() == 1) {
                String tableId = schema + "." + tableName;
                if (ToResolveContainer.TABLE_NOT_CONSISTENT_IN_SHARDINGS.contains(tableId)) {
                    AlertUtil.alertSelfResolve(AlarmCode.TABLE_NOT_CONSISTENT_IN_SHARDINGS, Alert.AlertLevel.WARN, AlertUtil.genSingleLabel("TABLE", tableId),
                            ToResolveContainer.TABLE_NOT_CONSISTENT_IN_SHARDINGS, tableId);
                }
                tableMeta = MetaHelper.initTableMeta(tableName, shardingNodeTableStructureSQLMap.keySet().iterator().next(), version, schema);
            }
            return tableMeta;
        }

        private synchronized void consistentWarning() {
            String errorMsg = "Table [" + tableName + "] structure are not consistent in different shardingNode!";
            logger.warn(errorMsg);
            AlertUtil.alertSelf(AlarmCode.TABLE_NOT_CONSISTENT_IN_SHARDINGS, Alert.AlertLevel.WARN, errorMsg, AlertUtil.genSingleLabel("TABLE", schema + "." + tableName));
            ToResolveContainer.TABLE_NOT_CONSISTENT_IN_SHARDINGS.add(schema + "." + tableName);
            logger.info("Currently detected: ");
            for (Map.Entry<String, List<String>> entry : shardingNodeTableStructureSQLMap.entrySet()) {
                StringBuilder stringBuilder = new StringBuilder();
                for (String dn : entry.getValue()) {
                    stringBuilder.append("shardingNode:[").append(dn).append("]");
                }
                stringBuilder.append(":").append(entry);
                logger.info(stringBuilder.toString());
            }
        }
    }
}
