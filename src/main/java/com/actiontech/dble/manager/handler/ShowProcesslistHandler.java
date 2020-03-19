package com.actiontech.dble.manager.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDataNode;
import com.actiontech.dble.backend.datasource.PhysicalDataSource;
import com.actiontech.dble.sqlengine.MultiRowSQLQueryResultHandler;
import com.actiontech.dble.sqlengine.SQLJob;
import com.actiontech.dble.sqlengine.SQLQueryResult;
import com.actiontech.dble.sqlengine.SQLQueryResultListener;
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
    private String dataNode;
    private List<Long> threadIds;
    private Map<String, Map<String, String>> result;
    private Lock lock;
    private Condition done;
    private boolean finished = false;
    private boolean success = false;

    public ShowProcesslistHandler(String dataNode, List<Long> threadIds) {
        this.dataNode = dataNode;
        this.threadIds = threadIds;
        this.lock = new ReentrantLock();
        this.done = lock.newCondition();
    }

    public void execute() {
        String sbSql = SQL.replace("{0}", StringUtils.join(threadIds, ','));
        PhysicalDataNode dn = DbleServer.getInstance().getConfig().getDataNodes().get(dataNode);
        PhysicalDataSource ds = dn.getDataHost().getWriteSource();
        if (ds.isAlive()) {
            MultiRowSQLQueryResultHandler resultHandler = new MultiRowSQLQueryResultHandler(MYSQL_SHOW_PROCESSLIST_COLS, new MySQLShowProcesslistListener());
            SQLJob sqlJob = new SQLJob(sbSql, dn.getDatabase(), resultHandler, ds);
            sqlJob.run();
        } else {
            MultiRowSQLQueryResultHandler resultHandler = new MultiRowSQLQueryResultHandler(MYSQL_SHOW_PROCESSLIST_COLS, new MySQLShowProcesslistListener());
            SQLJob sqlJob = new SQLJob(sbSql, dataNode, resultHandler, false);
            sqlJob.run();
        }
        waitDone();
    }

    void signalDone() {
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
                LOGGER.warn("execute 'show processlist' error in " + dataNode);
            } else {
                success = true;
                List<Map<String, String>> rows = res.getResult();
                result = new HashMap<>(rows.size(), 1f);
                for (Map<String, String> row : rows) {
                    String threadId = row.get(MYSQL_SHOW_PROCESSLIST_COLS[0]);
                    result.put(dataNode + "." + threadId, row);
                }
            }
            signalDone();
        }
    }
}
