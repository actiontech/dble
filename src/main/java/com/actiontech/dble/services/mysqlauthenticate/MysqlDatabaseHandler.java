/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.mysqlauthenticate;

import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.sqlengine.MultiRowSQLQueryResultHandler;
import com.actiontech.dble.sqlengine.OneTimeConnJob;
import com.actiontech.dble.sqlengine.SQLQueryResult;
import com.actiontech.dble.sqlengine.SQLQueryResultListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class MysqlDatabaseHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlDatabaseHandler.class);
    private static final String MYSQL_SHOW_DATABASES = "show databases";
    private final ReentrantLock lock = new ReentrantLock();
    private Map<String, PhysicalDbGroup> dbGroups;
    private Set<String> databases = new HashSet<>();
    private final Condition finishCond = lock.newCondition();
    private boolean isFinish = false;

    public MysqlDatabaseHandler(Map<String, PhysicalDbGroup> dbGroups) {
        this.dbGroups = dbGroups;
    }

    public Set<String> execute(String dbGroupName) {
        String mysqlShowDataBasesCols = "Database";
        MultiRowSQLQueryResultHandler resultHandler = new MultiRowSQLQueryResultHandler(new String[]{mysqlShowDataBasesCols}, new MySQLShowDatabasesListener(mysqlShowDataBasesCols));
        PhysicalDbInstance ds = getPhysicalDbInstance(dbGroupName);
        if (ds != null) {
            OneTimeConnJob sqlJob = new OneTimeConnJob(MYSQL_SHOW_DATABASES, null, resultHandler, ds);
            sqlJob.run();
            waitDone();
        } else {
            LOGGER.warn("No dbInstance is alive, server can not get 'show databases' result");
        }
        return databases;
    }

    private PhysicalDbInstance getPhysicalDbInstance(String dbGroupName) {
        PhysicalDbInstance ds = null;
        PhysicalDbGroup dbGroup = dbGroups.get(dbGroupName);
        PhysicalDbInstance dsTest;
        if (dbGroup != null) {
            dsTest = dbGroup.getWriteDbInstance();
            if (dsTest.isTestConnSuccess()) {
                ds = dsTest;
            }
        }
        return ds;
    }

    private void waitDone() {
        lock.lock();
        try {
            while (!isFinish) {
                finishCond.await();
            }
        } catch (InterruptedException e) {
            LOGGER.info("[MysqlDatabaseHandler] conn Interrupted: " + e);
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
                    String databaseName = row.get(mysqlShowDataBasesCol);
                    databases.add(databaseName);
                }
            }
            handleFinished();
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



