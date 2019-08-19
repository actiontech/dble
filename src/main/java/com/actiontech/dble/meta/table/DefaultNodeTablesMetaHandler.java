/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.meta.table;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDBNode;
import com.actiontech.dble.backend.datasource.PhysicalDatasource;
import com.actiontech.dble.meta.ReloadLogHelper;
import com.actiontech.dble.meta.protocol.StructureMeta;
import com.actiontech.dble.sqlengine.MultiRowSQLQueryResultHandler;
import com.actiontech.dble.sqlengine.MultiTablesMetaJob;
import com.actiontech.dble.sqlengine.SQLQueryResult;
import com.actiontech.dble.sqlengine.SQLQueryResultListener;

import java.util.List;
import java.util.Map;

public class DefaultNodeTablesMetaHandler {
    protected final ReloadLogHelper logger;
    private static final String[] MYSQL_SHOW_CREATE_TABLE_COLS = new String[]{
            "Table",
            "Create Table"};
    private static final String SQL = "show create table `{0}`;";
    private String dataNode;
    private AbstractSchemaMetaHandler schemaMetaHandler;
    private volatile List<String> tables;

    DefaultNodeTablesMetaHandler(AbstractSchemaMetaHandler schemaMetaHandler, List<String> tables, String dataNode, boolean isReload) {
        this.schemaMetaHandler = schemaMetaHandler;
        this.tables = tables;
        this.dataNode = dataNode;
        this.logger = new ReloadLogHelper(isReload);
    }

    public void execute() {
        StringBuilder sbSql = new StringBuilder();
        for (String table : tables) {
            sbSql.append(SQL.replace("{0}", table));
        }
        PhysicalDBNode dn = DbleServer.getInstance().getConfig().getDataNodes().get(dataNode);
        PhysicalDatasource ds = dn.getDbPool().getSource();
        if (ds.isAlive()) {
            logger.info("Ds is alive execute sql in singleNode:" + dataNode);
            MultiRowSQLQueryResultHandler resultHandler = new MultiRowSQLQueryResultHandler(MYSQL_SHOW_CREATE_TABLE_COLS, new MySQLShowCreateTablesListener());
            MultiTablesMetaJob sqlJob = new MultiTablesMetaJob(sbSql.toString(), dn.getDatabase(), resultHandler, ds, true);
            sqlJob.run();
        } else {
            logger.info("Ds is not alive execute sql in singleNode:" + dataNode);
            MultiRowSQLQueryResultHandler resultHandler = new MultiRowSQLQueryResultHandler(MYSQL_SHOW_CREATE_TABLE_COLS, new MySQLShowCreateTablesListener());
            MultiTablesMetaJob sqlJob = new MultiTablesMetaJob(sbSql.toString(), dataNode, resultHandler, false, true);
            sqlJob.run();
        }
    }


    private class MySQLShowCreateTablesListener implements SQLQueryResultListener<SQLQueryResult<List<Map<String, String>>>> {

        MySQLShowCreateTablesListener() {
        }

        @Override
        public void onResult(SQLQueryResult<List<Map<String, String>>> result) {
            List<Map<String, String>> rows = result.getResult();
            for (Map<String, String> row : rows) {
                String table = row.get(MYSQL_SHOW_CREATE_TABLE_COLS[0]);
                if (DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
                    table = table.toLowerCase();
                }
                tables.remove(table);
                String createSQL = row.get(MYSQL_SHOW_CREATE_TABLE_COLS[1]);
                StructureMeta.TableMeta tableMeta = MetaHelper.initTableMeta(table, createSQL, System.currentTimeMillis());
                schemaMetaHandler.handleSingleMetaData(tableMeta);
            }
            if (tables.size() > 0) {
                for (String table : tables) {
                    logger.warn("show create table " + table + " has no results");
                }
            }
            schemaMetaHandler.countDownSingleTable();
        }

    }
}
