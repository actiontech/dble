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
import com.actiontech.dble.backend.datasource.PhysicalDataNode;
import com.actiontech.dble.backend.datasource.PhysicalDataSource;
import com.actiontech.dble.meta.ReloadLogHelper;
import com.actiontech.dble.meta.protocol.StructureMeta;
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
    private List<String> dataNodes;
    private AtomicInteger nodesNumber;
    protected String schema;
    private Set<String> selfNode;
    private ConcurrentMap<String, List<String>> dataNodeTableStructureSQLMap;


    public AbstractTableMetaHandler(String schema, String tableName, List<String> dataNodes, Set<String> selfNode, boolean isReload) {
        this.dataNodes = dataNodes;
        this.nodesNumber = new AtomicInteger(dataNodes.size());
        this.schema = schema;
        this.selfNode = selfNode;
        this.tableName = tableName;
        this.dataNodeTableStructureSQLMap = new ConcurrentHashMap<>();
        this.logger = new ReloadLogHelper(isReload);
    }

    public void execute() {
        logger.info("table " + tableName + " execute start");
        for (String dataNode : dataNodes) {
            if (selfNode != null && selfNode.contains(dataNode)) {
                this.countdown();
                return;
            }
            PhysicalDataNode dn = DbleServer.getInstance().getConfig().getDataNodes().get(dataNode);
            PhysicalDataSource ds = dn.getDataHost().getWriteSource();
            String sql = SQL_PREFIX + "`" + tableName + "`";
            if (ds.isAlive()) {
                OneRawSQLQueryResultHandler resultHandler = new OneRawSQLQueryResultHandler(MYSQL_SHOW_CREATE_TABLE_COLS, new MySQLTableStructureListener(dataNode, System.currentTimeMillis(), ds));
                SQLJob sqlJob = new SQLJob(sql, dn.getDatabase(), resultHandler, ds);
                sqlJob.run();
            } else {
                OneRawSQLQueryResultHandler resultHandler = new OneRawSQLQueryResultHandler(MYSQL_SHOW_CREATE_TABLE_COLS, new MySQLTableStructureListener(dataNode, System.currentTimeMillis(), null));
                SQLJob sqlJob = new SQLJob(sql, dataNode, resultHandler, false);
                sqlJob.run();
            }
        }
    }

    protected abstract void countdown();

    protected abstract void handlerTable(StructureMeta.TableMeta tableMeta);

    private class MySQLTableStructureListener implements SQLQueryResultListener<SQLQueryResult<Map<String, String>>> {
        private String dataNode;
        private long version;
        private PhysicalDataSource ds;

        MySQLTableStructureListener(String dataNode, long version, PhysicalDataSource ds) {
            this.dataNode = dataNode;
            this.version = version;
            this.ds = ds;
        }

        @Override
        public void onResult(SQLQueryResult<Map<String, String>> result) {
            String tableId = "DataNode[" + dataNode + "]:Table[" + tableName + "]";
            logger.info(tableId + " on result " + result.isSuccess() + " count is " + nodesNumber);
            String key = null;
            if (ds != null) {
                key = "DataHost[" + ds.getHostConfig().getName() + "." + ds.getConfig().getHostName() + "],data_node[" + dataNode + "],schema[" + schema + "]";
            }
            if (!result.isSuccess()) {
                //not thread safe
                String warnMsg = "Can't get table " + tableName + "'s config from DataNode:" + dataNode + "! Maybe the table is not initialized!";
                logger.warn(warnMsg);
                AlertUtil.alertSelf(AlarmCode.TABLE_LACK, Alert.AlertLevel.WARN, warnMsg, AlertUtil.genSingleLabel("TABLE", tableId));
                ToResolveContainer.TABLE_LACK.add(tableId);
                if (nodesNumber.decrementAndGet() == 0) {
                    logger.info(tableId + " count down to 0 ,try to count down the table");
                    StructureMeta.TableMeta tableMeta = genTableMeta();
                    handlerTable(tableMeta);
                    countdown();
                }
                return;
            } else {
                if (ToResolveContainer.TABLE_LACK.contains(tableId)) {
                    AlertUtil.alertSelfResolve(AlarmCode.TABLE_LACK, Alert.AlertLevel.WARN, AlertUtil.genSingleLabel("TABLE", tableId),
                            ToResolveContainer.TABLE_LACK, tableId);
                }
                if (ds != null && ToResolveContainer.DATA_NODE_LACK.contains(key)) {
                    Map<String, String> labels = AlertUtil.genSingleLabel("data_host", ds.getHostConfig().getName() + "-" + ds.getConfig().getHostName());
                    labels.put("data_node", dataNode);
                    AlertUtil.alertResolve(AlarmCode.DATA_NODE_LACK, Alert.AlertLevel.WARN, "mysql", ds.getConfig().getId(), labels,
                            ToResolveContainer.DATA_NODE_LACK, key);
                }
            }

            String currentSql = result.getResult().get(MYSQL_SHOW_CREATE_TABLE_COLS[1]);
            if (dataNodeTableStructureSQLMap.containsKey(currentSql)) {
                List<String> dataNodeList = dataNodeTableStructureSQLMap.get(currentSql);
                dataNodeList.add(dataNode);
            } else {
                List<String> dataNodeList = new LinkedList<>();
                dataNodeList.add(dataNode);
                dataNodeTableStructureSQLMap.put(currentSql, dataNodeList);
            }

            if (nodesNumber.decrementAndGet() == 0) {
                logger.info(tableId + " count down to 0 ,try to count down the table");
                StructureMeta.TableMeta tableMeta = genTableMeta();
                handlerTable(tableMeta);
                countdown();
            }
        }

        private StructureMeta.TableMeta genTableMeta() {
            StructureMeta.TableMeta tableMeta = null;
            if (dataNodeTableStructureSQLMap.size() > 1) {
                // Through the SQL is different, the table Structure may still same.
                // for example: autoIncrement number
                Set<StructureMeta.TableMeta> tableMetas = new HashSet<>();
                for (String sql : dataNodeTableStructureSQLMap.keySet()) {
                    tableMeta = MetaHelper.initTableMeta(tableName, sql, version);
                    tableMetas.add(tableMeta);
                }
                String tableId = schema + "." + tableName;
                if (tableMetas.size() > 1) {
                    consistentWarning();
                } else if (ToResolveContainer.TABLE_NOT_CONSISTENT_IN_DATAHOSTS.contains(tableId)) {
                    AlertUtil.alertSelfResolve(AlarmCode.TABLE_NOT_CONSISTENT_IN_DATAHOSTS, Alert.AlertLevel.WARN, AlertUtil.genSingleLabel("TABLE", tableId),
                            ToResolveContainer.TABLE_NOT_CONSISTENT_IN_DATAHOSTS, tableId);
                }
                tableMetas.clear();
            } else if (dataNodeTableStructureSQLMap.size() == 1) {
                String tableId = schema + "." + tableName;
                if (ToResolveContainer.TABLE_NOT_CONSISTENT_IN_DATAHOSTS.contains(tableId)) {
                    AlertUtil.alertSelfResolve(AlarmCode.TABLE_NOT_CONSISTENT_IN_DATAHOSTS, Alert.AlertLevel.WARN, AlertUtil.genSingleLabel("TABLE", tableId),
                            ToResolveContainer.TABLE_NOT_CONSISTENT_IN_DATAHOSTS, tableId);
                }
                tableMeta = MetaHelper.initTableMeta(tableName, dataNodeTableStructureSQLMap.keySet().iterator().next(), version);
            }
            return tableMeta;
        }

        private synchronized void consistentWarning() {
            String errorMsg = "Table [" + tableName + "] structure are not consistent in different data node!";
            logger.warn(errorMsg);
            AlertUtil.alertSelf(AlarmCode.TABLE_NOT_CONSISTENT_IN_DATAHOSTS, Alert.AlertLevel.WARN, errorMsg, AlertUtil.genSingleLabel("TABLE", schema + "." + tableName));
            ToResolveContainer.TABLE_NOT_CONSISTENT_IN_DATAHOSTS.add(schema + "." + tableName);
            logger.info("Currently detected: ");
            for (Map.Entry<String, List<String>> entry : dataNodeTableStructureSQLMap.entrySet()) {
                StringBuilder stringBuilder = new StringBuilder();
                for (String dn : entry.getValue()) {
                    stringBuilder.append("DataNode:[").append(dn).append("]");
                }
                stringBuilder.append(":").append(entry);
                logger.info(stringBuilder.toString());
            }
        }
    }
}
