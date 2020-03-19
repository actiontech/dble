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
import com.actiontech.dble.sqlengine.MultiRowSQLQueryResultHandler;
import com.actiontech.dble.sqlengine.MultiTablesMetaJob;
import com.actiontech.dble.sqlengine.SQLQueryResult;
import com.actiontech.dble.sqlengine.SQLQueryResultListener;

import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class GetTableMetaHandler {
    private static final String MYSQL_TABLE_COLS = "Table";
    private static final String MYSQL_VIEW_COLS = "View";
    private static final String MYSQL_CREATE_TABLE_COLS = "Create Table";
    private static final String MYSQL_CREATE_VIEW_COLS = "Create View";
    private static final String[] MYSQL_SHOW_CREATE_TABLE_COLS = new String[]{
            MYSQL_TABLE_COLS,
            MYSQL_VIEW_COLS,
            MYSQL_CREATE_TABLE_COLS,
            MYSQL_CREATE_VIEW_COLS,
            "character_set_client",
    };
    private static final String SQL_SHOW_CREATE_TABLE = "show create table `{0}`;";
    protected final ReloadLogHelper logger;
    protected String schema;

    GetTableMetaHandler(String schema, boolean isReload) {
        this.schema = schema;
        this.logger = new ReloadLogHelper(isReload);
    }

    public void execute(String dataNode, Set<String> tables) {
        StringBuilder sbSql = new StringBuilder();
        for (String table : tables) {
            sbSql.append(SQL_SHOW_CREATE_TABLE.replace("{0}", table));
        }
        PhysicalDataNode dn = DbleServer.getInstance().getConfig().getDataNodes().get(dataNode);
        PhysicalDataSource ds = dn.getDataHost().getWriteSource();
        if (ds.isAlive()) {
            logger.info("Datasource is alive start sqljob for dataNode:" + dataNode);
            MultiRowSQLQueryResultHandler resultHandler = new MultiRowSQLQueryResultHandler(MYSQL_SHOW_CREATE_TABLE_COLS, new TableStructureListener(dataNode, tables, ds));
            MultiTablesMetaJob sqlJob = new MultiTablesMetaJob(sbSql.toString(), dn.getDatabase(), resultHandler, ds, logger.isReload());
            sqlJob.run();
        } else {
            logger.info("Datasource is not alive start sqljob for dataNode:" + dataNode);
            MultiRowSQLQueryResultHandler resultHandler = new MultiRowSQLQueryResultHandler(MYSQL_SHOW_CREATE_TABLE_COLS, new TableStructureListener(dataNode, tables, null));
            MultiTablesMetaJob sqlJob = new MultiTablesMetaJob(sbSql.toString(), dataNode, resultHandler, false, logger.isReload());
            sqlJob.run();
        }
    }

    abstract void countdown(String dataNode, Set<String> tables);

    abstract void handleTable(String dataNode, String table, boolean isView, String sql);

    private class TableStructureListener implements SQLQueryResultListener<SQLQueryResult<List<Map<String, String>>>> {
        private String dataNode;
        private PhysicalDataSource ds;
        private Set<String> expectedTables;

        TableStructureListener(String dataNode, Set<String> expectedTables, PhysicalDataSource ds) {
            this.dataNode = dataNode;
            this.expectedTables = expectedTables;
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

            String table;
            String createSQL;
            List<Map<String, String>> rows = result.getResult();
            for (Map<String, String> row : rows) {
                boolean isView = false;
                // when table is view, we select view,create_view and character_set_client
                // but if not, we only select table, create_table
                if (row.size() < 3) {
                    table = row.get(MYSQL_TABLE_COLS);
                    createSQL = row.get(MYSQL_CREATE_TABLE_COLS);
                } else {
                    isView = true;
                    table = row.get(MYSQL_VIEW_COLS);
                    createSQL = row.get(MYSQL_CREATE_VIEW_COLS);
                }
                if (DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
                    table = table.toLowerCase();
                }
                expectedTables.remove(table);
                handleTable(dataNode, table, isView, createSQL);
            }

            logger.info("dataNode normally count down:" + dataNode + " for schema " + schema);
            countdown(dataNode, expectedTables);
        }
    }

}
