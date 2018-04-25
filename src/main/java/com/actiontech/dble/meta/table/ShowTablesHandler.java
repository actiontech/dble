/*
 * Copyright (C) 2016-2018 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.meta.table;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDBNode;
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

public class ShowTablesHandler {
    protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractTableMetaHandler.class);
    private static final String SQL = "show tables ";
    private SchemaConfig config;
    private MultiTableMetaHandler multiTableMetaHandler;
    private volatile List<String> tables = new ArrayList<>();
    private volatile boolean finished = false;

    public List<String> getTables() {
        return tables;
    }

    public boolean isFinished() {
        return finished;
    }

    ShowTablesHandler(MultiTableMetaHandler multiTableMetaHandler, SchemaConfig config) {
        this.multiTableMetaHandler = multiTableMetaHandler;
        this.config = config;
    }

    public void execute() {
        PhysicalDBNode dn = DbleServer.getInstance().getConfig().getDataNodes().get(config.getDataNode());
        String mysqlShowTableCol = "Tables_in_" + dn.getDatabase();
        String[] mysqlShowTableCols = new String[]{mysqlShowTableCol};
        MultiRowSQLQueryResultHandler resultHandler = new MultiRowSQLQueryResultHandler(mysqlShowTableCols, new MySQLShowTablesListener(mysqlShowTableCol));
        SQLJob sqlJob = new SQLJob(SQL, config.getDataNode(), resultHandler, false);
        sqlJob.run();
    }


    private class MySQLShowTablesListener implements SQLQueryResultListener<SQLQueryResult<List<Map<String, String>>>> {
        private String mysqlShowTableCol;

        MySQLShowTablesListener(String mysqlShowTableCol) {
            this.mysqlShowTableCol = mysqlShowTableCol;
        }

        @Override
        public void onResult(SQLQueryResult<List<Map<String, String>>> result) {
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
