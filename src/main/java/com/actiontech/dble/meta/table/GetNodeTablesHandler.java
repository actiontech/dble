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
import com.actiontech.dble.backend.datasource.ShardingNode;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.sqlengine.MultiRowSQLQueryResultHandler;
import com.actiontech.dble.sqlengine.SQLJob;
import com.actiontech.dble.sqlengine.SQLQueryResult;
import com.actiontech.dble.sqlengine.SQLQueryResultListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class GetNodeTablesHandler {
    protected static final Logger LOGGER = LoggerFactory.getLogger(GetNodeTablesHandler.class);
    protected static final String SQL = "show full tables where Table_type ='BASE TABLE' ";
    private static final String SQL_WITH_VIEW = "show full tables ";
    protected String shardingNode;
    protected boolean isFinished = false;
    protected Lock lock = new ReentrantLock();
    protected Condition notify = lock.newCondition();
    private String sql = SQL;

    GetNodeTablesHandler(String shardingNode, boolean skipView) {
        this.shardingNode = shardingNode;
        if (!skipView) {
            sql = SQL_WITH_VIEW;
        }
    }

    GetNodeTablesHandler(String shardingNode) {
        this(shardingNode, true);
    }

    public void execute() {
        ShardingNode dn = DbleServer.getInstance().getConfig().getShardingNodes().get(shardingNode);
        String mysqlShowTableCol = "Tables_in_" + dn.getDatabase();
        String[] mysqlShowTableCols = new String[]{mysqlShowTableCol, "Table_type"};
        PhysicalDbInstance ds = dn.getDbGroup().getWriteSource();
        if (ds.isAlive()) {
            MultiRowSQLQueryResultHandler resultHandler = new MultiRowSQLQueryResultHandler(mysqlShowTableCols, new MySQLShowTablesListener(mysqlShowTableCol, dn.getDatabase(), ds));
            SQLJob sqlJob = new SQLJob(sql, dn.getDatabase(), resultHandler, ds);
            sqlJob.run();
        } else {
            MultiRowSQLQueryResultHandler resultHandler = new MultiRowSQLQueryResultHandler(mysqlShowTableCols, new MySQLShowTablesListener(mysqlShowTableCol, dn.getDatabase(), null));
            SQLJob sqlJob = new SQLJob(sql, shardingNode, resultHandler, false);
            sqlJob.run();
        }
    }

    protected abstract void handleTable(String table, String tableType);

    protected void handleFinished() {
        lock.lock();
        try {
            isFinished = true;
            notify.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private class MySQLShowTablesListener implements SQLQueryResultListener<SQLQueryResult<List<Map<String, String>>>> {
        private String mysqlShowTableCol;
        private PhysicalDbInstance ds;
        private String schema;

        MySQLShowTablesListener(String mysqlShowTableCol, String schema, PhysicalDbInstance ds) {
            this.mysqlShowTableCol = mysqlShowTableCol;
            this.ds = ds;
            this.schema = schema;
        }

        @Override
        public void onResult(SQLQueryResult<List<Map<String, String>>> result) {
            String key = null;
            if (ds != null) {
                key = "dbInstance[" + ds.getHostConfig().getName() + "." + ds.getConfig().getInstanceName() + "],sharding_node[" + shardingNode + "],schema[" + schema + "]";
            }
            if (!result.isSuccess()) {
                //not thread safe
                String warnMsg = "Can't show tables from DataNode:" + shardingNode + "! Maybe the data node is not initialized!";
                LOGGER.warn(warnMsg);
                if (ds != null) {
                    Map<String, String> labels = AlertUtil.genSingleLabel("dbInstance", ds.getHostConfig().getName() + "-" + ds.getConfig().getInstanceName());
                    labels.put("sharding_node", shardingNode);
                    AlertUtil.alert(AlarmCode.SHARDING_NODE_LACK, Alert.AlertLevel.WARN, "{" + key + "} is lack", "mysql", ds.getConfig().getId(), labels);
                    ToResolveContainer.SHARDING_NODE_LACK.add(key);
                }
                handleFinished();
                return;
            }
            if (ds != null && ToResolveContainer.SHARDING_NODE_LACK.contains(key)) {
                Map<String, String> labels = AlertUtil.genSingleLabel("dbInstance", ds.getHostConfig().getName() + "-" + ds.getConfig().getInstanceName());
                labels.put("sharding_node", shardingNode);
                AlertUtil.alertResolve(AlarmCode.SHARDING_NODE_LACK, Alert.AlertLevel.WARN, "mysql", ds.getConfig().getId(), labels,
                        ToResolveContainer.SHARDING_NODE_LACK, key);
            }
            List<Map<String, String>> rows = result.getResult();
            for (Map<String, String> row : rows) {
                String table = row.get(mysqlShowTableCol);
                String type = row.get("Table_type");
                if (DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
                    table = table.toLowerCase();
                }
                handleTable(table, type);
            }
            handleFinished();
        }
    }

}
