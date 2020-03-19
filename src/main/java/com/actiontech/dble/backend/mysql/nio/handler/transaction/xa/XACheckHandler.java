package com.actiontech.dble.backend.mysql.nio.handler.transaction.xa;

import com.actiontech.dble.backend.datasource.PhysicalDataSource;
import com.actiontech.dble.sqlengine.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public final class XACheckHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(XACheckHandler.class);
    private static final String[] MYSQL_RECOVER_COLS = new String[]{"data"};
    private final String xid;
    private final String schema;
    private final String dataNode;
    private final PhysicalDataSource ds;
    private Lock lock;
    private Condition done;
    private boolean finished = false;

    // status flag
    private boolean isExistXid = false;
    private boolean isSuccess = true;

    public XACheckHandler(String xid, String schema, String dataNode, PhysicalDataSource ds) {
        this.xid = xid;
        this.ds = ds;
        this.schema = schema;
        this.dataNode = dataNode;
        this.lock = new ReentrantLock();
        this.done = lock.newCondition();
    }

    public void checkXid() {
        final String sql = "XA RECOVER";
        MultiRowSQLQueryResultHandler resultHandler = new MultiRowSQLQueryResultHandler(MYSQL_RECOVER_COLS, new CheckXidExistenceListener());
        if (ds.isAlive()) {
            SQLJob sqlJob = new SQLJob(sql, schema, resultHandler, ds);
            sqlJob.run();
        } else {
            SQLJob sqlJob = new SQLJob(sql, dataNode, resultHandler, false);
            sqlJob.run();
        }
        waitDone();
    }

    public void killXaThread(long oldThreadId) {
        final String sql = "KILL CONNECTION " + oldThreadId;
        OneRawSQLQueryResultHandler resultHandler = new OneRawSQLQueryResultHandler(new String[0], new KillXaThreadListener(oldThreadId));
        if (ds.isAlive()) {
            SQLJob sqlJob = new SQLJob(sql, schema, resultHandler, ds);
            sqlJob.run();
        } else {
            SQLJob sqlJob = new SQLJob(sql, dataNode, resultHandler, false);
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
            LOGGER.info("check xa grapping done " + e);
        } finally {
            lock.unlock();
        }
    }

    public boolean isExistXid() {
        return isExistXid;
    }

    public boolean isSuccess() {
        return isSuccess;
    }

    private class CheckXidExistenceListener implements SQLQueryResultListener<SQLQueryResult<List<Map<String, String>>>> {

        CheckXidExistenceListener() {
        }

        @Override
        public void onResult(SQLQueryResult<List<Map<String, String>>> result) {
            if (!result.isSuccess()) {
                // error
                LOGGER.warn("execute 'XA RECOVER' in " + ds.getDataHost().getHostName() + " error!");
                isSuccess = false;
            } else if (!result.getResult().isEmpty()) {
                List<Map<String, String>> xaRows = result.getResult();
                for (Map<String, String> row : xaRows) {
                    String tempXid = row.get("data");
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("check xid is " + xid + " tmp xid is " + tempXid);
                    }
                    if (tempXid.equalsIgnoreCase(xid.replace("\'", ""))) {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("find xid!check xid is " + xid + " tmp xid is " + tempXid);
                        }
                        isExistXid = true;
                        break;
                    }
                }
            }
            signalDone();
        }
    }

    private class KillXaThreadListener implements SQLQueryResultListener<SQLQueryResult<Map<String, String>>> {

        private final long threadId;

        KillXaThreadListener(long threadId) {
            this.threadId = threadId;
        }

        @Override
        public void onResult(SQLQueryResult<Map<String, String>> result) {
            if (!result.isSuccess()) {
                // error, ignore
                LOGGER.warn("execute KILL CONNECTION " + threadId + " in " + ds.getName() + " error!");
            }
            signalDone();
        }
    }
}
