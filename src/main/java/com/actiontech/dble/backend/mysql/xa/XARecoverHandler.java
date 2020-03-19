package com.actiontech.dble.backend.mysql.xa;

import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.backend.datasource.PhysicalDataSource;
import com.actiontech.dble.manager.handler.ShowProcesslistHandler;
import com.actiontech.dble.sqlengine.OneRawSQLQueryResultHandler;
import com.actiontech.dble.sqlengine.SQLJob;
import com.actiontech.dble.sqlengine.SQLQueryResult;
import com.actiontech.dble.sqlengine.SQLQueryResultListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class XARecoverHandler {

    protected static final Logger LOGGER = LoggerFactory.getLogger(ShowProcesslistHandler.class);
    private boolean isCommit;
    private ParticipantLogEntry logEntry;
    private Lock lock;
    private Condition done;
    private boolean finished = false;
    private boolean success = false;

    public XARecoverHandler(boolean isCommit, ParticipantLogEntry logEntry) {
        this.isCommit = isCommit;
        this.logEntry = logEntry;
        this.lock = new ReentrantLock();
        this.done = lock.newCondition();
    }

    public void execute(String sql, String db, PhysicalDataSource ds) {
        OneRawSQLQueryResultHandler resultHandler = new OneRawSQLQueryResultHandler(new String[0], new XARecoverCallback(isCommit, logEntry));
        SQLJob sqlJob = new SQLJob(sql, db, resultHandler, ds);
        sqlJob.run();
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
            LOGGER.info("wait 'xa commit/rollback' grapping done " + e);
        } finally {
            lock.unlock();
        }
    }

    public boolean isSuccess() {
        return success;
    }

    class XARecoverCallback implements SQLQueryResultListener<SQLQueryResult<Map<String, String>>> {

        private ParticipantLogEntry logEntry;
        private String operator;
        private TxState txState;

        XARecoverCallback(boolean isCommit, ParticipantLogEntry logEntry) {
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
                success = true;
            } else {
                String msg = String.format("[CALLBACK][XA %s host:%s port:%s scheme:%s state:%s ] when server start,but failed.Please check backend mysql.", logEntry.getCoordinatorId(), logEntry.getHost(), logEntry.getPort(), logEntry.getSchema(), txState.getState());
                LOGGER.warn(msg);
                Map<String, String> labels = AlertUtil.genSingleLabel("data_host", "operator " + operator);
                AlertUtil.alertSelf(AlarmCode.XA_RECOVER_FAIL, Alert.AlertLevel.WARN, msg, labels);
            }
            signalDone();
        }
    }

}
