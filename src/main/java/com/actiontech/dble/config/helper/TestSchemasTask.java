/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.helper;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.alarm.ToResolveContainer;
import com.actiontech.dble.backend.datasource.PhysicalDataSource;
import com.actiontech.dble.plan.common.ptr.BoolPtr;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.sqlengine.MultiRowSQLQueryResultHandler;
import com.actiontech.dble.sqlengine.OneTimeConnJob;
import com.actiontech.dble.sqlengine.SQLQueryResult;
import com.actiontech.dble.sqlengine.SQLQueryResultListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class TestSchemasTask extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestSchemasTask.class);
    private PhysicalDataSource ds;
    private BoolPtr boolPtr;
    private Set<String> errKeys;
    private Map<String, String> nodes = new HashMap<>();
    private boolean needAlert;
    private ReentrantLock lock = new ReentrantLock();
    private volatile boolean isFinish = false;
    private Condition finishCond = lock.newCondition();

    public TestSchemasTask(PhysicalDataSource ds, List<Pair<String, String>> nodeList, Set<String> errKeys, BoolPtr boolPtr, boolean needAlert) {
        this.ds = ds;
        this.errKeys = errKeys;
        this.boolPtr = boolPtr;
        this.needAlert = needAlert;
        for (Pair<String, String> node : nodeList) {
            nodes.put(node.getValue(), node.getKey()); // schema->node
        }

    }

    public Map<String, String> getNodes() {
        return nodes;
    }

    @Override
    public void run() {
        String mysqlShowDataBasesCols = "Database";
        MultiRowSQLQueryResultHandler resultHandler = new MultiRowSQLQueryResultHandler(new String[]{mysqlShowDataBasesCols}, new MySQLShowDatabasesListener(mysqlShowDataBasesCols));
        OneTimeConnJob sqlJob = new OneTimeConnJob("show databases", null, resultHandler, ds);
        sqlJob.run();
        lock.lock();
        try {
            while (!isFinish) {
                finishCond.await();
            }
        } catch (InterruptedException e) {
            LOGGER.warn("test conn Interrupted:", e);
        } finally {
            lock.unlock();
        }
    }

    private class MySQLShowDatabasesListener implements SQLQueryResultListener<SQLQueryResult<List<Map<String, String>>>> {
        private String mysqlShowDataBasesCol;

        MySQLShowDatabasesListener(String mysqlShowDataBasesCol) {
            this.mysqlShowDataBasesCol = mysqlShowDataBasesCol;
        }

        @Override
        public void onResult(SQLQueryResult<List<Map<String, String>>> result) {
            if (result.isSuccess()) {
                List<Map<String, String>> rows = result.getResult();
                for (Map<String, String> row : rows) {
                    String schema = row.get(mysqlShowDataBasesCol);
                    if (DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
                        schema = schema.toLowerCase();
                    }
                    String nodeName = nodes.remove(schema);
                    if (nodeName != null) {
                        String key = "DataHost[" + ds.getHostConfig().getName() + "." + ds.getConfig().getHostName() + "],data_node[" + nodeName + "],schema[" + schema + "]";
                        LOGGER.info("SelfCheck### test " + key + " database connection success ");
                    }
                }
            }
            reportSchemaNotFound();
            handleFinished();
        }

        private void reportSchemaNotFound() {
            for (Map.Entry<String, String> node : nodes.entrySet()) {
                boolPtr.set(false);
                String nodeName = node.getValue();
                String key = "DataHost[" + ds.getHostConfig().getName() + "." + ds.getConfig().getHostName() + "],data_node[" + nodeName + "],schema[" + node.getKey() + "]";
                errKeys.add(key);
                LOGGER.warn("test conn " + key + " error");
                if (needAlert) {
                    Map<String, String> labels = AlertUtil.genSingleLabel("data_host", ds.getHostConfig().getName() + "-" + ds.getConfig().getHostName());
                    labels.put("data_node", nodeName);
                    AlertUtil.alert(AlarmCode.DATA_NODE_LACK, Alert.AlertLevel.WARN, "{" + key + "} is lack", "mysql", ds.getConfig().getId(), labels);
                    ToResolveContainer.DATA_NODE_LACK.add(key);
                }
            }
        }

        private void handleFinished() {
            lock.lock();
            try {
                isFinish = true;
                finishCond.signal();
            } finally {
                lock.unlock();
            }
        }
    }
}
