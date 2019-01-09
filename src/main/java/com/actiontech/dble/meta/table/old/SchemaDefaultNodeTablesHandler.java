/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.meta.table.old;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.alarm.ToResolveContainer;
import com.actiontech.dble.backend.datasource.PhysicalDBNode;
import com.actiontech.dble.backend.datasource.PhysicalDatasource;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.sqlengine.MultiRowSQLQueryResultHandler;
import com.actiontech.dble.sqlengine.SQLJob;
import com.actiontech.dble.sqlengine.SQLQueryResult;
import com.actiontech.dble.sqlengine.SQLQueryResultListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SchemaDefaultNodeTablesHandler {
    protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractTableMetaHandler.class);
    private static final String SQL = "show full tables where Table_type ='BASE TABLE'  ";
    private SchemaConfig config;
    private String dataNode;
    private MultiTableMetaHandler multiTableMetaHandler;
    private volatile List<String> tables = new ArrayList<>();
    private volatile boolean finished = false;

    public List<String> getTables() {
        return tables;
    }

    public boolean isFinished() {
        return finished;
    }

    SchemaDefaultNodeTablesHandler(MultiTableMetaHandler multiTableMetaHandler, SchemaConfig config) {
        this.multiTableMetaHandler = multiTableMetaHandler;
        this.config = config;
        this.dataNode = config.getDataNode();
    }

    public void execute() {
        PhysicalDBNode dn = DbleServer.getInstance().getConfig().getDataNodes().get(dataNode);
        String mysqlShowTableCol = "Tables_in_" + dn.getDatabase();
        String[] mysqlShowTableCols = new String[]{mysqlShowTableCol};
        PhysicalDatasource ds = dn.getDbPool().getSource();
        if (ds.isAlive()) {
            MultiRowSQLQueryResultHandler resultHandler = new MultiRowSQLQueryResultHandler(mysqlShowTableCols, new MySQLShowTablesListener(mysqlShowTableCol, dn.getDatabase(), ds));
            SQLJob sqlJob = new SQLJob(SQL, dn.getDatabase(), resultHandler, ds);
            sqlJob.run();
        } else {
            MultiRowSQLQueryResultHandler resultHandler = new MultiRowSQLQueryResultHandler(mysqlShowTableCols, new MySQLShowTablesListener(mysqlShowTableCol, dn.getDatabase(), null));
            SQLJob sqlJob = new SQLJob(SQL, dataNode, resultHandler, false);
            sqlJob.run();
        }
    }


    private class MySQLShowTablesListener implements SQLQueryResultListener<SQLQueryResult<List<Map<String, String>>>> {
        private String mysqlShowTableCol;
        private PhysicalDatasource ds;
        private String schema;

        MySQLShowTablesListener(String mysqlShowTableCol, String schema, PhysicalDatasource ds) {
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
                finished = true;
                multiTableMetaHandler.showTablesFinished();
                return;
            }
            if (ds != null && ToResolveContainer.DATA_NODE_LACK.contains(key)) {
                Map<String, String> labels = AlertUtil.genSingleLabel("data_host", ds.getHostConfig().getName() + "-" + ds.getConfig().getHostName());
                labels.put("data_node", dataNode);
                if (AlertUtil.alertResolve(AlarmCode.DATA_NODE_LACK, Alert.AlertLevel.WARN, "mysql", ds.getConfig().getId(), labels)) {
                    ToResolveContainer.DATA_NODE_LACK.remove(key);
                }
            }
            List<Map<String, String>> rows = result.getResult();
            for (Map<String, String> row : rows) {
                String table = row.get(mysqlShowTableCol);
                if (DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
                    table = table.toLowerCase();
                }
                if (!config.getTables().containsKey(table)) {
                    tables.add(table);
                }
            }
            finished = true;
            multiTableMetaHandler.showTablesFinished();
        }
    }
}
