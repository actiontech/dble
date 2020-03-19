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
    protected String dataNode;
    protected boolean isFinished = false;
    protected Lock lock = new ReentrantLock();
    protected Condition notify = lock.newCondition();
    private String sql = SQL;

    GetNodeTablesHandler(String dataNode, boolean skipView) {
        this.dataNode = dataNode;
        if (!skipView) {
            sql = SQL_WITH_VIEW;
        }
    }

    GetNodeTablesHandler(String dataNode) {
        this(dataNode, true);
    }

    public void execute() {
        PhysicalDataNode dn = DbleServer.getInstance().getConfig().getDataNodes().get(dataNode);
        String mysqlShowTableCol = "Tables_in_" + dn.getDatabase();
        String[] mysqlShowTableCols = new String[]{mysqlShowTableCol, "Table_type"};
        PhysicalDataSource ds = dn.getDataHost().getWriteSource();
        if (ds.isAlive()) {
            MultiRowSQLQueryResultHandler resultHandler = new MultiRowSQLQueryResultHandler(mysqlShowTableCols, new MySQLShowTablesListener(mysqlShowTableCol, dn.getDatabase(), ds));
            SQLJob sqlJob = new SQLJob(sql, dn.getDatabase(), resultHandler, ds);
            sqlJob.run();
        } else {
            MultiRowSQLQueryResultHandler resultHandler = new MultiRowSQLQueryResultHandler(mysqlShowTableCols, new MySQLShowTablesListener(mysqlShowTableCol, dn.getDatabase(), null));
            SQLJob sqlJob = new SQLJob(sql, dataNode, resultHandler, false);
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
        private PhysicalDataSource ds;
        private String schema;

        MySQLShowTablesListener(String mysqlShowTableCol, String schema, PhysicalDataSource ds) {
            this.mysqlShowTableCol = mysqlShowTableCol;
            this.ds = ds;
            this.schema = schema;
        }

        @Override
        public void onResult(SQLQueryResult<List<Map<String, String>>> result) {
            String key = null;
            if (ds != null) {
                key = "DataHost[" + ds.getHostConfig().getName() + "." + ds.getConfig().getHostName() + "],data_node[" + dataNode + "],schema[" + schema + "]";
            }
            if (!result.isSuccess()) {
                //not thread safe
                String warnMsg = "Can't show tables from DataNode:" + dataNode + "! Maybe the data node is not initialized!";
                LOGGER.warn(warnMsg);
                if (ds != null) {
                    Map<String, String> labels = AlertUtil.genSingleLabel("data_host", ds.getHostConfig().getName() + "-" + ds.getConfig().getHostName());
                    labels.put("data_node", dataNode);
                    AlertUtil.alert(AlarmCode.DATA_NODE_LACK, Alert.AlertLevel.WARN, "{" + key + "} is lack", "mysql", ds.getConfig().getId(), labels);
                    ToResolveContainer.DATA_NODE_LACK.add(key);
                }
                handleFinished();
                return;
            }
            if (ds != null && ToResolveContainer.DATA_NODE_LACK.contains(key)) {
                Map<String, String> labels = AlertUtil.genSingleLabel("data_host", ds.getHostConfig().getName() + "-" + ds.getConfig().getHostName());
                labels.put("data_node", dataNode);
                AlertUtil.alertResolve(AlarmCode.DATA_NODE_LACK, Alert.AlertLevel.WARN, "mysql", ds.getConfig().getId(), labels,
                        ToResolveContainer.DATA_NODE_LACK, key);
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
