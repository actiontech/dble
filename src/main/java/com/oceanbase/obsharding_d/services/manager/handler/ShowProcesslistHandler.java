/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.handler;

import com.oceanbase.obsharding_d.backend.datasource.PhysicalDbInstance;
import com.oceanbase.obsharding_d.sqlengine.MultiRowSQLQueryResultHandler;
import com.oceanbase.obsharding_d.sqlengine.SQLJob;
import com.oceanbase.obsharding_d.sqlengine.SQLQueryResult;
import com.oceanbase.obsharding_d.sqlengine.SQLQueryResultListener;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ShowProcesslistHandler {
    protected static final Logger LOGGER = LoggerFactory.getLogger(ShowProcesslistHandler.class);
    private static final String[] MYSQL_SHOW_PROCESSLIST_COLS = new String[]{
            "Id", "db", "Command", "Time", "State", "Info"};
    private static final String SQL = "SELECT Id,db,Command,Time,State,Info FROM information_schema.processlist where Id in ({0});";
    private final PhysicalDbInstance dbInstance;
    private final List<Long> threadIds;
    private Map<String, Map<String, String>> result;
    private final Lock lock;
    private final Condition done;
    private boolean finished = false;
    private boolean success = false;

    public ShowProcesslistHandler(PhysicalDbInstance dbInstance, List<Long> threadIds) {
        this.dbInstance = dbInstance;
        this.threadIds = threadIds;
        this.lock = new ReentrantLock();
        this.done = lock.newCondition();
    }

    public void execute() {
        String sbSql = SQL.replace("{0}", StringUtils.join(threadIds, ','));
        if (dbInstance.isAlive()) {
            MultiRowSQLQueryResultHandler resultHandler = new MultiRowSQLQueryResultHandler(MYSQL_SHOW_PROCESSLIST_COLS, new MySQLShowProcesslistListener());
            SQLJob sqlJob = new SQLJob(sbSql, null, resultHandler, dbInstance);
            sqlJob.run();
        }
        waitDone();
    }

    private void signalDone() {
        lock.lock();
        try {
            finished = true;
            done.signal();
        } finally {
            lock.unlock();
        }
    }

    private void waitDone() {
        lock.lock();
        try {
            while (!finished) {
                done.await();
            }
        } catch (InterruptedException e) {
            LOGGER.info("wait 'show processlist' grapping done " + e);
        } finally {
            lock.unlock();
        }
    }

    public Map<String, Map<String, String>> getResult() {
        return this.result;
    }

    public boolean isSuccess() {
        return success;
    }

    private class MySQLShowProcesslistListener implements SQLQueryResultListener<SQLQueryResult<List<Map<String, String>>>> {
        MySQLShowProcesslistListener() {
        }

        @Override
        public void onResult(SQLQueryResult<List<Map<String, String>>> res) {
            if (!res.isSuccess()) {
                LOGGER.warn("execute 'show processlist' error in " + dbInstance.getName());
            } else {
                success = true;
                List<Map<String, String>> rows = res.getResult();
                result = new HashMap<>(rows.size());
                for (Map<String, String> row : rows) {
                    String threadId = row.get(MYSQL_SHOW_PROCESSLIST_COLS[0]);
                    result.put(dbInstance.getName() + "." + threadId, row);
                }
            }
            signalDone();
        }
    }
}
