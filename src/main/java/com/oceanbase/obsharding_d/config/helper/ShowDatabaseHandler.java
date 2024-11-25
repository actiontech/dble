/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.config.helper;

import com.oceanbase.obsharding_d.backend.datasource.PhysicalDbGroup;
import com.oceanbase.obsharding_d.backend.datasource.PhysicalDbInstance;
import com.oceanbase.obsharding_d.sqlengine.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ShowDatabaseHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(ShowDatabaseHandler.class);
    private final ReentrantLock lock = new ReentrantLock();
    private Map<String, PhysicalDbGroup> dbGroups;
    private Set<String> databases = new HashSet<>();
    private final Condition finishCond = lock.newCondition();
    private boolean isFinish = false;
    private String showDatabases = "show databases";
    private String showDataBasesCols;
    private SQLJob sqlJob;


    public ShowDatabaseHandler(Map<String, PhysicalDbGroup> dbGroups, String showDataBasesCols) {
        this.dbGroups = dbGroups;
        this.showDataBasesCols = showDataBasesCols;
    }

    private void reset() {
        isFinish = false;
        databases.clear();
    }

    public Set<String> execute(String dbGroupName) {
        reset();
        MultiRowSQLQueryResultHandler resultHandler = new MultiRowSQLQueryResultHandler(new String[]{showDataBasesCols}, new ShowDatabasesListener(showDataBasesCols));
        PhysicalDbInstance ds = getPhysicalDbInstance(dbGroupName);
        if (ds != null) {
            sqlJob = new SQLJob(showDatabases, null, resultHandler, ds);
            sqlJob.run();
            waitDone();
        } else {
            LOGGER.warn("No dbInstance is alive, server can not get 'show databases' result");
        }
        return new HashSet<>(databases);
    }


    // for dryrun
    public Set<String> execute(PhysicalDbInstance ds) {
        reset();
        MultiRowSQLQueryResultHandler resultHandler = new MultiRowSQLQueryResultHandler(new String[]{showDataBasesCols}, new ShowDatabasesListener(showDataBasesCols));
        if (ds != null) {
            sqlJob = new OneTimeConnJob(showDatabases, null, resultHandler, ds);
            sqlJob.run();
            waitDone();
        } else {
            LOGGER.warn("No dbInstance is alive, server can not get 'show databases' result");
        }
        return new HashSet<>(databases);
    }

    private PhysicalDbInstance getPhysicalDbInstance(String dbGroupName) {
        PhysicalDbInstance ds = null;
        try {
            PhysicalDbGroup dbGroup = dbGroups.get(dbGroupName);
            if (dbGroup != null) {
                ds = dbGroup.rwSelect(null, false);
            }
        } catch (IOException e) {
            LOGGER.warn("select dbInstance error", e);
        }
        return ds;
    }

    public List<PhysicalDbInstance> getPhysicalDbInstances() {
        List<PhysicalDbInstance> physicalDbInstanceList = new ArrayList<>();
        for (PhysicalDbGroup dbGroup : dbGroups.values()) {
            for (PhysicalDbInstance dsTest : dbGroup.getDbInstances(false)) {
                if (dsTest.isTestConnSuccess()) {
                    physicalDbInstanceList.add(dsTest);
                }
            }
        }
        return physicalDbInstanceList;
    }

    private void waitDone() {
        lock.lock();
        try {
            while (!isFinish) {
                finishCond.await();
            }
        } catch (InterruptedException e) {
            LOGGER.info("[MysqlDatabaseHandler] conn Interrupted: " + e);
            if (sqlJob != null) {
                sqlJob.terminate("thread interrupted");
            }
            throw new IllegalStateException(e);
        } finally {
            lock.unlock();
        }
    }

    private class ShowDatabasesListener implements SQLQueryResultListener<SQLQueryResult<List<Map<String, String>>>> {
        private String showDataBasesCol;

        ShowDatabasesListener(String clickhouseShowDataBasesCol) {
            this.showDataBasesCol = clickhouseShowDataBasesCol;
        }

        @Override
        public void onResult(SQLQueryResult<List<Map<String, String>>> result) {
            if (result.isSuccess()) {
                List<Map<String, String>> rows = result.getResult();
                for (Map<String, String> row : rows) {
                    String databaseName = row.get(showDataBasesCol);
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



