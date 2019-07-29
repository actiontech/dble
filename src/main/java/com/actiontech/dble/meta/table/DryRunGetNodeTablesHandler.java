/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.meta.table;

import com.actiontech.dble.backend.datasource.PhysicalDBNode;
import com.actiontech.dble.backend.datasource.PhysicalDatasource;
import com.actiontech.dble.config.ErrorInfo;
import com.actiontech.dble.sqlengine.*;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by szf on 2018/9/20.
 */
public class DryRunGetNodeTablesHandler extends GetNodeTablesHandler {

    private final AtomicInteger counter;
    private final PhysicalDBNode phyDataNode;
    private final Map<String, Set<String>> returnMap;
    private final boolean isLowerCase;
    private final List<ErrorInfo> list;

    public DryRunGetNodeTablesHandler(AtomicInteger counter, PhysicalDBNode phyDataNode, Map<String, Set<String>> returnMap, boolean isLowerCase, List<ErrorInfo> list) {
        super(phyDataNode.getName());
        this.counter = counter;
        this.phyDataNode = phyDataNode;
        this.returnMap = returnMap;
        this.isLowerCase = isLowerCase;
        this.list = list;
    }

    @Override
    public void execute() {
        String mysqlShowTableCol = "Tables_in_" + phyDataNode.getDatabase();
        String[] mysqlShowTableCols = new String[]{mysqlShowTableCol};
        PhysicalDatasource[] dsList = phyDataNode.getDbPool().getSources();
        PhysicalDatasource ds = null;
        if (dsList != null) {
            for (PhysicalDatasource tds : dsList) {
                if (tds.isTestConnSuccess()) {
                    ds = tds;
                    break;
                }
            }
        }
        if (ds != null) {
            MultiRowSQLQueryResultHandler resultHandler = new MultiRowSQLQueryResultHandler(mysqlShowTableCols, new MySQLShowTablesListener(mysqlShowTableCol));
            SpecialSqlJob sqlJob = new SpecialSqlJob(SQL, phyDataNode.getDatabase(), resultHandler, ds, list);
            sqlJob.run();
        } else {
            list.add(new ErrorInfo("Backend", "WARNING", "dataNode[" + phyDataNode.getName() + "] has no available writeHost,The table in this dataNode has not checked"));
            handleFinished();
        }
    }

    @Override
    protected void handleTables(String table) {
        returnMap.get(phyDataNode.getName()).add(table);
    }

    @Override
    protected void handleFinished() {
        counter.decrementAndGet();
    }


    private class MySQLShowTablesListener implements SQLQueryResultListener<SQLQueryResult<List<Map<String, String>>>> {
        private String mysqlShowTableCol;

        MySQLShowTablesListener(String mysqlShowTableCol) {
            this.mysqlShowTableCol = mysqlShowTableCol;
        }

        @Override
        public void onResult(SQLQueryResult<List<Map<String, String>>> result) {
            if (!result.isSuccess()) {
                String warnMsg = "Can't show tables from DataNode:" + phyDataNode + "! Maybe the data node is not initialized!";
                LOGGER.warn(warnMsg);
                handleFinished();
                return;
            }
            returnMap.put(phyDataNode.getName(), new HashSet<String>());
            List<Map<String, String>> rows = result.getResult();
            for (Map<String, String> row : rows) {
                String table = row.get(mysqlShowTableCol);
                if (isLowerCase) {
                    table = table.toLowerCase();
                }
                handleTables(table);
            }
            handleFinished();
        }
    }
}
