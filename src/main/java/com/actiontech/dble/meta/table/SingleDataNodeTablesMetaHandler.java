/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.meta.table;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.alarm.ToResolveContainer;
import com.actiontech.dble.backend.datasource.PhysicalDBNode;
import com.actiontech.dble.backend.datasource.PhysicalDatasource;
import com.actiontech.dble.meta.ReloadLogHelper;
import com.actiontech.dble.sqlengine.MultiRowSQLQueryResultHandler;
import com.actiontech.dble.sqlengine.MultiTablesMetaJob;
import com.actiontech.dble.sqlengine.SQLQueryResult;
import com.actiontech.dble.sqlengine.SQLQueryResultListener;

import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SingleDataNodeTablesMetaHandler {
    protected final ReloadLogHelper logger;
    private static final String[] MYSQL_SHOW_CREATE_TABLE_COLS = new String[]{
            "Table",
            "Create Table"};
    private static final String SQL = "show create table `{0}`;";

    private Map<String, Set<String>> dataNodeMap = new HashMap<>();
    protected String schema;
    private Set<String> selfNode;
    private Lock showTablesLock = new ReentrantLock();
    private Condition collectTables = showTablesLock.newCondition();
    private AbstractSchemaMetaHandler schemaMetaHandler;

    public SingleDataNodeTablesMetaHandler(AbstractSchemaMetaHandler schemaMetaHandler, String schema, Map<String, Set<String>> dataNodeMap, Set<String> selfNode, boolean isReload) {
        this.dataNodeMap = dataNodeMap;
        this.schema = schema;
        this.selfNode = selfNode;
        this.logger = new ReloadLogHelper(isReload);
        this.schemaMetaHandler = schemaMetaHandler;
    }

    public void execute() {
        for (Map.Entry<String, Set<String>> dataNodeInfo : dataNodeMap.entrySet()) {
            String dataNode = dataNodeInfo.getKey();
            if (selfNode != null && selfNode.contains(dataNode)) {
                logger.info("the Node " + dataNode + " is a selfNode,count down");
                this.countdown(dataNode);
                continue;
            }
            Set<String> existTables = listExistTables(dataNode, dataNodeInfo.getValue());
            if (existTables.size() == 0) {
                logger.info("the Node " + dataNode + " has no exist table,count down");
                this.countdown(dataNode);
                continue;
            }
            StringBuilder sbSql = new StringBuilder();
            for (String table : existTables) {
                sbSql.append(SQL.replace("{0}", table));
            }
            PhysicalDBNode dn = DbleServer.getInstance().getConfig().getDataNodes().get(dataNode);
            PhysicalDatasource ds = dn.getDbPool().getSource();
            if (ds.isAlive()) {
                logger.info("Datasource is alive start sqljob for dataNode:" + dataNode);
                MultiRowSQLQueryResultHandler resultHandler = new MultiRowSQLQueryResultHandler(MYSQL_SHOW_CREATE_TABLE_COLS, new MySQLShowCreateTablesListener(existTables, dataNode, ds));
                MultiTablesMetaJob sqlJob = new MultiTablesMetaJob(sbSql.toString(), dn.getDatabase(), resultHandler, ds, logger.isReload());
                sqlJob.run();
            } else {
                logger.info("Datasource is not alive start sqljob for dataNode:" + dataNode);
                MultiRowSQLQueryResultHandler resultHandler = new MultiRowSQLQueryResultHandler(MYSQL_SHOW_CREATE_TABLE_COLS, new MySQLShowCreateTablesListener(existTables, dataNode, null));
                MultiTablesMetaJob sqlJob = new MultiTablesMetaJob(sbSql.toString(), dataNode, resultHandler, false, logger.isReload());
                sqlJob.run();
            }
        }
    }

    private Set<String> listExistTables(String dataNode, Set<String> tables) {
        GetSpecialNodeTablesHandler showTablesHandler = new GetSpecialNodeTablesHandler(this, tables, dataNode);
        showTablesHandler.execute();
        showTablesLock.lock();
        try {
            while (!showTablesHandler.isFinished()) {
                collectTables.await();
            }
        } catch (InterruptedException e) {
            logger.info("getSingleTables " + e);
            return new HashSet<>();
        } finally {
            showTablesLock.unlock();
        }
        return showTablesHandler.getExistsTables();
    }

    protected void countdown(String dataNode) {
        schemaMetaHandler.countDownShardTable(dataNode);
    }

    protected void handlerTable(String table, String dataNode, String sql) {
        schemaMetaHandler.checkTableConsistent(table, dataNode, sql);
    }


    void showTablesFinished() {
        showTablesLock.lock();
        try {
            collectTables.signal();
        } finally {
            showTablesLock.unlock();
        }
    }

    private class MySQLShowCreateTablesListener implements SQLQueryResultListener<SQLQueryResult<List<Map<String, String>>>> {
        private Set<String> tables;
        private String dataNode;
        private PhysicalDatasource ds;

        MySQLShowCreateTablesListener(Set<String> tables, String dataNode, PhysicalDatasource ds) {
            this.tables = tables;
            this.dataNode = dataNode;
            this.ds = ds;
        }

        @Override
        public void onResult(SQLQueryResult<List<Map<String, String>>> result) {
            String key = null;
            if (ds != null) {
                key = "DataHost[" + ds.getHostConfig().getName() + "." + ds.getConfig().getHostName() + "],data_node[" + dataNode + "],schema[" + schema + "]";
            }
            if (ds != null && ToResolveContainer.DATA_NODE_LACK.contains(key)) {
                Map<String, String> labels = AlertUtil.genSingleLabel("data_host", ds.getHostConfig().getName() + "-" + ds.getConfig().getHostName());
                labels.put("data_node", dataNode);
                AlertUtil.alertResolve(AlarmCode.DATA_NODE_LACK, Alert.AlertLevel.WARN, "mysql", ds.getConfig().getId(), labels,
                        ToResolveContainer.DATA_NODE_LACK, key);
            }
            List<Map<String, String>> rows = result.getResult();
            for (Map<String, String> row : rows) {
                String table = row.get(MYSQL_SHOW_CREATE_TABLE_COLS[0]);
                if (DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
                    table = table.toLowerCase();
                }
                tables.remove(table);
                String createSQL = row.get(MYSQL_SHOW_CREATE_TABLE_COLS[1]);
                handlerTable(table, dataNode, createSQL);

            }
            if (tables.size() > 0) {
                for (String table : tables) {
                    String tableId = "DataNode[" + dataNode + "]:Table[" + table + "]";
                    String warnMsg = "Can't get table " + table + "'s config from DataNode:" + dataNode + "! Maybe the table is not initialized!";
                    logger.warn(warnMsg);
                    AlertUtil.alertSelf(AlarmCode.TABLE_LACK, Alert.AlertLevel.WARN, warnMsg, AlertUtil.genSingleLabel("TABLE", tableId));
                    ToResolveContainer.TABLE_LACK.add(tableId);
                }
            }
            logger.info("dataNode normally count down:" + dataNode + " for schema " + schema);
            countdown(dataNode);
        }
    }

}
