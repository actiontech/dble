/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction.xa;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.alarm.AlarmCode;
import com.oceanbase.obsharding_d.alarm.Alert;
import com.oceanbase.obsharding_d.alarm.AlertUtil;
import com.oceanbase.obsharding_d.backend.datasource.PhysicalDbGroup;
import com.oceanbase.obsharding_d.backend.datasource.PhysicalDbInstance;
import com.oceanbase.obsharding_d.backend.mysql.xa.ParticipantLogEntry;
import com.oceanbase.obsharding_d.backend.mysql.xa.TxState;
import com.oceanbase.obsharding_d.backend.mysql.xa.XAStateLog;
import com.oceanbase.obsharding_d.sqlengine.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class XAHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(XAHandler.class);
    private static final String XARECOVER_SQL = "XA RECOVER";
    private static final String KILL_SQL = "KILL CONNECTION ";
    private static final String[] MYSQL_RECOVER_COLS = new String[]{"formatID", "gtrid_length", "bqual_length", "data"};
    private final Map<PhysicalDbInstance, List<Map<String, String>>> results = new ConcurrentHashMap<>(8);
    private final List<SQLJob> sqlJobs = new ArrayList<>();
    private final AtomicInteger count = new AtomicInteger();
    private final Lock lock;
    private final Condition done;
    private final PhysicalDbInstance pd;
    private boolean isSuccess = false;

    // default all write dbInstance
    public XAHandler() {
        this.lock = new ReentrantLock();
        this.done = lock.newCondition();
        this.pd = null;

        fillDbInstance(
                getShardingWriteDbInstance());
    }

    // need specified dbInstance
    public XAHandler(PhysicalDbInstance pd) {
        this.lock = new ReentrantLock();
        this.done = lock.newCondition();
        this.pd = pd;
        fillDbInstance(pd);
    }

    private void fillDbInstance(PhysicalDbInstance... dss) {
        for (PhysicalDbInstance pdi : dss) {
            MultiRowSQLQueryResultHandler resultHandler = new MultiRowSQLQueryResultHandler(MYSQL_RECOVER_COLS, new XARecover(pdi));
            SQLJob sqlJob;
            if (pdi.isAlive()) {
                sqlJob = new SQLJob(XARECOVER_SQL, null, resultHandler, pdi);
                sqlJobs.add(sqlJob);
            } else {
                LOGGER.warn("When prepare execute 'XA RECOVER' in {}, check it's isAlive is false!", pdi);
                results.put(pdi, new ArrayList<>());
            }
        }
    }

    // single|mulit
    protected void checkXA() {
        count.set(sqlJobs.size());
        for (SQLJob sqlJob : sqlJobs) {
            sqlJob.run();
        }
        waitAllJobDone();
    }

    // single
    public void killThread(long threadId) {
        if (pd != null) {
            count.set(1);
            String sql = KILL_SQL + threadId;
            OneRawSQLQueryResultHandler resultHandler = new OneRawSQLQueryResultHandler(new String[0], new KillThread(pd, threadId));
            if (pd.isAlive()) {
                SQLJob sqlJob = new SQLJob(sql, null, resultHandler, pd);
                sqlJob.run();
                waitAllJobDone();
            } else {
                LOGGER.warn("When prepare execute 'KILL CONNECTION {}' in {}, check it's isAlive is false!", threadId, pd);
            }
        }
    }

    // single
    public void executeXaCmd(String cmd, boolean isCommit, ParticipantLogEntry logEntry) {
        if (pd != null) {
            count.set(1);
            OneRawSQLQueryResultHandler resultHandler = new OneRawSQLQueryResultHandler(new String[0], new XACmdCallback(isCommit, logEntry));
            if (pd.isAlive()) {
                SQLJob sqlJob = new SQLJob(cmd, null, resultHandler, pd);
                sqlJob.run();
                waitAllJobDone();
            } else {
                LOGGER.warn("When prepare execute '{}' in {} , check it's isAlive is false!", cmd, pd);
            }
        }
    }

    protected Map<PhysicalDbInstance, List<Map<String, String>>> getXAResults() {
        return results;
    }

    private void waitAllJobDone() {
        lock.lock();
        try {
            while (count.get() != 0) {
                done.await();
            }
        } catch (InterruptedException e) {
            LOGGER.warn("execute XAHandler interrupted", e);
        } finally {
            lock.unlock();
        }
    }

    private void signalDone() {
        lock.lock();
        try {
            if (count.decrementAndGet() == 0) {
                done.signal();
            }
        } finally {
            lock.unlock();
        }
    }

    public boolean isSuccess() {
        return isSuccess;
    }

    private PhysicalDbInstance[] getShardingWriteDbInstance() {
        List<PhysicalDbInstance> dss = new ArrayList<>();
        for (PhysicalDbGroup dbGroup : OBsharding_DServer.getInstance().getConfig().getDbGroups().values()) {
            if (!dbGroup.isShardingUseless()) {
                dss.add(dbGroup.getWriteDbInstance());
            }
        }
        return dss.toArray(new PhysicalDbInstance[0]);
    }

    final class XARecover implements SQLQueryResultListener<SQLQueryResult<List<Map<String, String>>>> {
        PhysicalDbInstance pdi;

        private XARecover(PhysicalDbInstance pd) {
            this.pdi = pd;
        }

        @Override
        public void onResult(SQLQueryResult<List<Map<String, String>>> result) {
            if (!result.isSuccess()) {
                LOGGER.warn("execute 'XA RECOVER' in {} error!", pdi);
                results.put(pdi, new ArrayList<>());
            } else {
                isSuccess = true;
                results.put(pdi, result.getResult());
            }
            signalDone();
        }
    }

    class KillThread implements SQLQueryResultListener<SQLQueryResult<Map<String, String>>> {
        PhysicalDbInstance pdi;
        private final long threadId;

        KillThread(PhysicalDbInstance pd, long threadId) {
            this.pdi = pd;
            this.threadId = threadId;
        }

        @Override
        public void onResult(SQLQueryResult<Map<String, String>> result) {
            if (!result.isSuccess()) {
                LOGGER.warn("execute 'KILL CONNECTION {}' in {} error!", threadId, pdi);
            } else {
                isSuccess = true;
            }
            signalDone();
        }
    }

    class XACmdCallback implements SQLQueryResultListener<SQLQueryResult<Map<String, String>>> {
        private final ParticipantLogEntry logEntry;
        private final String operator;
        private final TxState txState;

        XACmdCallback(boolean isCommit, ParticipantLogEntry logEntry) {
            if (isCommit) {
                operator = "COMMIT";
                txState = TxState.TX_COMMITTED_STATE;
            } else {
                operator = "ROLLBACK";
                txState = TxState.TX_ROLLBACKED_STATE;
            }

            if (LOGGER.isDebugEnabled()) {
                String prepareDelayTime = System.getProperty("XA_RECOVERY_DELAY");
                long delayTime = prepareDelayTime == null ? 0 : Long.parseLong(prepareDelayTime) * 1000;
                //if using the debug log & using the jvm xa delay properties action will be delay by properties
                if (delayTime > 0) {
                    try {
                        LOGGER.debug("before xa recovery sleep time = " + delayTime);
                        Thread.sleep(delayTime);
                        LOGGER.debug("before xa recovery sleep finished " + delayTime);
                    } catch (Exception e) {
                        LOGGER.debug("before xa recovery sleep exception " + delayTime);
                    }
                }
            }
            this.logEntry = logEntry;
        }

        @Override
        public void onResult(SQLQueryResult<Map<String, String>> res) {
            if (res.isSuccess()) {
                LOGGER.debug(String.format("[CALLBACK][XA %s %s] when server start", operator, logEntry.getCoordinatorId()));
                XAStateLog.updateXARecoveryLog(logEntry.getCoordinatorId(), logEntry.getHost(), logEntry.getPort(), logEntry.getSchema(), logEntry.getExpires(), txState);
                XAStateLog.writeCheckpoint(logEntry.getCoordinatorId());
                isSuccess = true;
            } else {
                String msg = String.format("[CALLBACK][XA %s host:%s port:%s scheme:%s state:%s ] when server start,but failed.Please check backend mysql.", logEntry.getCoordinatorId(), logEntry.getHost(), logEntry.getPort(), logEntry.getSchema(), txState.getState());
                LOGGER.warn(msg);
                Map<String, String> labels = AlertUtil.genSingleLabel("dbInstance", "operator " + operator);
                AlertUtil.alertSelf(AlarmCode.XA_RECOVER_FAIL, Alert.AlertLevel.WARN, msg, labels);
            }
            signalDone();
        }
    }
}
