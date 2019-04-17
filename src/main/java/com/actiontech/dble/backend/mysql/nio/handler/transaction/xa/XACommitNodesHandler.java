/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.transaction.xa;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.AbstractCommitNodesHandler;
import com.actiontech.dble.backend.mysql.xa.CoordinatorLogEntry;
import com.actiontech.dble.backend.mysql.xa.ParticipantLogEntry;
import com.actiontech.dble.backend.mysql.xa.TxState;
import com.actiontech.dble.backend.mysql.xa.XAStateLog;
import com.actiontech.dble.btrace.provider.XaDelayProvider;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.util.StringUtil;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.actiontech.dble.config.ErrorCode.ER_ERROR_DURING_COMMIT;

public class XACommitNodesHandler extends AbstractCommitNodesHandler {
    private static final int COMMIT_TIMES = 5;
    private int tryCommitTimes = 0;
    private int backgroundCommitTimes = 0;
    private ParticipantLogEntry[] participantLogEntry = null;
    byte[] sendData = OkPacket.OK;

    private Lock lockForErrorHandle = new ReentrantLock();
    private Condition sendFinished = lockForErrorHandle.newCondition();
    private volatile boolean sendFinishedFlag = false;
    private ConcurrentMap<Object, Long> xaOldThreadIds;

    public XACommitNodesHandler(NonBlockingSession session) {
        super(session);
        xaOldThreadIds = new ConcurrentHashMap<>(session.getTargetCount());
    }

    @Override
    public void commit() {
        final int initCount = session.getTargetCount();
        lock.lock();
        try {
            reset(initCount);
        } finally {
            lock.unlock();
        }
        int position = 0;
        //get session's lock before sending commit(in fact, after ended)
        //then the XA transaction will be not killed, if killed ,then we will not commit
        if (session.getXaState() != null && session.getXaState() == TxState.TX_ENDED_STATE) {
            if (!session.cancelableStatusSet(NonBlockingSession.CANCEL_STATUS_COMMITTING)) {
                return;
            }
        }

        try {
            sendFinishedFlag = false;
            for (RouteResultsetNode rrn : session.getTargetKeys()) {
                final BackendConnection conn = session.getTarget(rrn);
                conn.setResponseHandler(this);
                if (!executeCommit((MySQLConnection) conn, position++)) {
                    break;
                }
            }
        } finally {
            lockForErrorHandle.lock();
            try {
                sendFinishedFlag = true;
                sendFinished.signalAll();
            } finally {
                lockForErrorHandle.unlock();
            }
        }

    }

    @Override
    public void clearResources() {
        tryCommitTimes = 0;
        backgroundCommitTimes = 0;
        participantLogEntry = null;
        sendData = OkPacket.OK;
        xaOldThreadIds.clear();
        if (closedConnSet != null) {
            closedConnSet.clear();
        }
    }

    @Override
    protected boolean executeCommit(MySQLConnection mysqlCon, int position) {
        TxState state = session.getXaState();
        if (state == TxState.TX_STARTED_STATE) {
            if (participantLogEntry == null) {
                participantLogEntry = new ParticipantLogEntry[nodeCount];
                CoordinatorLogEntry coordinatorLogEntry = new CoordinatorLogEntry(session.getSessionXaID(), participantLogEntry, session.getXaState());
                XAStateLog.flushMemoryRepository(session.getSessionXaID(), coordinatorLogEntry);
            }
            XAStateLog.initRecoveryLog(session.getSessionXaID(), position, mysqlCon);
            endPhase(mysqlCon);
        } else if (state == TxState.TX_ENDED_STATE) {
            if (position == 0) {
                if (!XAStateLog.saveXARecoveryLog(session.getSessionXaID(), TxState.TX_PREPARING_STATE)) {
                    String errMsg = "saveXARecoveryLog error, the stage is TX_PREPARING_STATE";
                    this.setFail(errMsg);
                    sendData = makeErrorPacket(errMsg);
                    nextParse();
                    return false;
                }
                this.debugCommitDelay();
            }
            preparePhase(mysqlCon);
        } else if (state == TxState.TX_PREPARED_STATE) {
            if (position == 0) {
                if (!XAStateLog.saveXARecoveryLog(session.getSessionXaID(), TxState.TX_COMMITTING_STATE)) {
                    String errMsg = "saveXARecoveryLog error, the stage is TX_COMMITTING_STATE";
                    this.setFail(errMsg);
                    sendData = makeErrorPacket(errMsg);
                    nextParse();
                    return false;
                }
                this.debugCommitDelay();
            }

            commitPhase(mysqlCon);
        } else if (state == TxState.TX_COMMIT_FAILED_STATE) {
            if (position == 0) {
                XAStateLog.saveXARecoveryLog(session.getSessionXaID(), TxState.TX_COMMIT_FAILED_STATE);
            }
            commitPhase(mysqlCon);
        } else if (state == TxState.TX_PREPARE_UNCONNECT_STATE) {
            final String errorMsg = this.error;
            LOGGER.warn("commit error and rollback the xa");
            if (decrementCountBy(1)) {
                DbleServer.getInstance().getComplexQueryExecutor().execute(new Runnable() {
                    @Override
                    public void run() {
                        ErrorPacket error = new ErrorPacket();
                        error.setErrNo(ER_ERROR_DURING_COMMIT);
                        error.setMessage(errorMsg == null ? "unknown error".getBytes() : errorMsg.getBytes());
                        XAAutoRollbackNodesHandler nextHandler = new XAAutoRollbackNodesHandler(session, error.toBytes(), null, null);
                        nextHandler.rollback();
                    }
                });
            }
        }
        return true;
    }

    private byte[] makeErrorPacket(String errMsg) {
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.setPacketId(1);
        errPacket.setErrNo(ErrorCode.ER_UNKNOWN_ERROR);
        errPacket.setMessage(StringUtil.encode(errMsg, session.getSource().getCharset().getResults()));
        return errPacket.toBytes();
    }

    private void endPhase(MySQLConnection mysqlCon) {
        String xaTxId = mysqlCon.getConnXID(session);
        String rrnName = ((RouteResultsetNode) mysqlCon.getAttachment()).getName();
        XaDelayProvider.delayBeforeXaEnd(rrnName, xaTxId);
        mysqlCon.execCmd("XA END " + xaTxId);
    }

    private void preparePhase(MySQLConnection mysqlCon) {
        String xaTxId = mysqlCon.getConnXID(session);
        String rrnName = ((RouteResultsetNode) mysqlCon.getAttachment()).getName();
        XaDelayProvider.delayBeforeXaPrepare(rrnName, xaTxId);
        mysqlCon.execCmd("XA PREPARE " + xaTxId);
    }

    private void commitPhase(MySQLConnection mysqlCon) {
        if (session.getXaState() == TxState.TX_COMMIT_FAILED_STATE) {
            MySQLConnection newConn = session.freshConn(mysqlCon, this);
            if (!newConn.equals(mysqlCon)) {
                xaOldThreadIds.putIfAbsent(mysqlCon.getAttachment(), mysqlCon.getThreadId());
                mysqlCon = newConn;
            } else if (decrementCountBy(1)) {
                cleanAndFeedback(false);
                return;
            }
        }
        String xaTxId = mysqlCon.getConnXID(session);
        String rrnName = ((RouteResultsetNode) mysqlCon.getAttachment()).getName();
        XaDelayProvider.delayBeforeXaCommit(rrnName, xaTxId);
        mysqlCon.execCmd("XA COMMIT " + xaTxId);
    }

    @Override
    public void okResponse(byte[] ok, BackendConnection conn) {
        this.waitUntilSendFinish();
        MySQLConnection mysqlCon = (MySQLConnection) conn;
        TxState state = mysqlCon.getXaStatus();
        if (state == TxState.TX_STARTED_STATE) {
            mysqlCon.setXaStatus(TxState.TX_ENDED_STATE);
            XAStateLog.saveXARecoveryLog(session.getSessionXaID(), mysqlCon);
            if (decrementCountBy(1)) {
                session.setXaState(TxState.TX_ENDED_STATE);
                nextParse();
            }
        } else if (state == TxState.TX_ENDED_STATE) {
            //PREPARE OK
            mysqlCon.setXaStatus(TxState.TX_PREPARED_STATE);
            XAStateLog.saveXARecoveryLog(session.getSessionXaID(), mysqlCon);
            if (decrementCountBy(1)) {
                if (session.getXaState() == TxState.TX_ENDED_STATE) {
                    session.setXaState(TxState.TX_PREPARED_STATE);
                }
                nextParse();
            }
        } else if (state == TxState.TX_COMMIT_FAILED_STATE || state == TxState.TX_PREPARED_STATE) {
            //COMMIT OK
            // XA reset status now
            mysqlCon.setXaStatus(TxState.TX_COMMITTED_STATE);
            XAStateLog.saveXARecoveryLog(session.getSessionXaID(), mysqlCon);
            mysqlCon.setXaStatus(TxState.TX_INITIALIZE_STATE);
            if (decrementCountBy(1)) {
                if (session.getXaState() == TxState.TX_PREPARED_STATE) {
                    session.setXaState(TxState.TX_INITIALIZE_STATE);
                }
                cleanAndFeedback(true);
            }
        }
    }

    @Override
    public void errorResponse(byte[] err, BackendConnection conn) {
        this.waitUntilSendFinish();
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.read(err);
        String errMsg = new String(errPacket.getMessage());
        this.setFail(errMsg);
        sendData = makeErrorPacket(errMsg);
        if (conn instanceof MySQLConnection) {
            MySQLConnection mysqlCon = (MySQLConnection) conn;
            if (mysqlCon.getXaStatus() == TxState.TX_STARTED_STATE) {
                mysqlCon.quit();
                mysqlCon.setXaStatus(TxState.TX_CONN_QUIT);
                XAStateLog.saveXARecoveryLog(session.getSessionXaID(), mysqlCon);
                if (decrementCountBy(1)) {
                    session.setXaState(TxState.TX_ENDED_STATE);
                    nextParse();
                }

                // 'xa prepare' error
            } else if (mysqlCon.getXaStatus() == TxState.TX_ENDED_STATE) {
                mysqlCon.quit();
                mysqlCon.setXaStatus(TxState.TX_CONN_QUIT);
                XAStateLog.saveXARecoveryLog(session.getSessionXaID(), mysqlCon);
                if (decrementCountBy(1)) {
                    if (session.getXaState() == TxState.TX_ENDED_STATE) {
                        session.setXaState(TxState.TX_PREPARED_STATE);
                    }
                    nextParse();
                }
                // 'xa commit' err
            } else if (mysqlCon.getXaStatus() == TxState.TX_PREPARED_STATE) { //TODO:service degradation?
                mysqlCon.setXaStatus(TxState.TX_COMMIT_FAILED_STATE);
                XAStateLog.saveXARecoveryLog(session.getSessionXaID(), mysqlCon);
                session.setXaState(TxState.TX_COMMIT_FAILED_STATE);
                if (decrementCountBy(1)) {
                    cleanAndFeedback(false);
                }
            } else if (mysqlCon.getXaStatus() == TxState.TX_COMMIT_FAILED_STATE) {
                if (errPacket.getErrNo() == ErrorCode.ER_XAER_NOTA) {
                    String xid = mysqlCon.getConnXID(session);
                    XACheckHandler handler = new XACheckHandler(xid, mysqlCon.getSchema(), mysqlCon.getPool().getDbPool().getSource());
                    // if mysql connection holding xa transaction wasn't released, may result in ER_XAER_NOTA.
                    // so we need check xid here
                    handler.checkXid();
                    if (handler.isSuccess() && !handler.isExistXid()) {
                        // Unknown XID ,if xa transaction only contains select statement, xid will lost after restart server although prepared
                        mysqlCon.setXaStatus(TxState.TX_COMMITTED_STATE);
                        XAStateLog.saveXARecoveryLog(session.getSessionXaID(), mysqlCon);
                        mysqlCon.setXaStatus(TxState.TX_INITIALIZE_STATE);
                        if (decrementCountBy(1)) {
                            if (session.getXaState() == TxState.TX_PREPARED_STATE) {
                                session.setXaState(TxState.TX_INITIALIZE_STATE);
                            }
                            cleanAndFeedback(false);
                        }
                    } else {
                        if (handler.isExistXid()) {
                            // kill mysql connection holding xa transaction, so current xa transaction can be committed next time.
                            handler.killXaThread(xaOldThreadIds.get(mysqlCon.getAttachment()));
                        }
                        XAStateLog.saveXARecoveryLog(session.getSessionXaID(), mysqlCon);
                        session.setXaState(TxState.TX_COMMIT_FAILED_STATE);
                        if (decrementCountBy(1)) {
                            cleanAndFeedback(false);
                        }
                    }
                } else {
                    mysqlCon.setXaStatus(TxState.TX_COMMIT_FAILED_STATE);
                    XAStateLog.saveXARecoveryLog(session.getSessionXaID(), mysqlCon);
                    session.setXaState(TxState.TX_COMMIT_FAILED_STATE);
                    if (decrementCountBy(1)) {
                        cleanAndFeedback(false);
                    }
                }
            }
        }
    }

    @Override
    public void connectionError(Throwable e, BackendConnection conn) {
        this.waitUntilSendFinish();
        LOGGER.info("backend connect", e);
        String errMsg = new String(StringUtil.encode(e.getMessage(), session.getSource().getCharset().getResults()));
        this.setFail(errMsg);
        sendData = makeErrorPacket(errMsg);
        innerConnectError(conn);
    }

    @Override
    public void connectionClose(final BackendConnection conn, final String reason) {
        this.waitUntilSendFinish();
        if (checkClosedConn(conn)) {
            return;
        }
        this.setFail(reason);
        sendData = makeErrorPacket(reason);
        innerConnectError(conn);
    }

    private void innerConnectError(BackendConnection conn) {
        if (conn instanceof MySQLConnection) {
            MySQLConnection mysqlCon = (MySQLConnection) conn;
            if (mysqlCon.getXaStatus() == TxState.TX_STARTED_STATE) {
                mysqlCon.quit();
                mysqlCon.setXaStatus(TxState.TX_CONN_QUIT);
                XAStateLog.saveXARecoveryLog(session.getSessionXaID(), mysqlCon);
                if (decrementCountBy(1)) {
                    session.setXaState(TxState.TX_ENDED_STATE);
                    nextParse();
                }
                //  'xa prepare' connectionClose,conn has quit
            } else if (mysqlCon.getXaStatus() == TxState.TX_ENDED_STATE) {
                mysqlCon.setXaStatus(TxState.TX_PREPARE_UNCONNECT_STATE);
                XAStateLog.saveXARecoveryLog(session.getSessionXaID(), mysqlCon);
                session.setXaState(TxState.TX_PREPARE_UNCONNECT_STATE);
                if (decrementCountBy(1)) {
                    nextParse();
                }
                // 'xa commit' connectionClose
            } else if (mysqlCon.getXaStatus() == TxState.TX_COMMIT_FAILED_STATE || mysqlCon.getXaStatus() == TxState.TX_PREPARED_STATE) { //TODO:service degradation?
                mysqlCon.setXaStatus(TxState.TX_COMMIT_FAILED_STATE);
                XAStateLog.saveXARecoveryLog(session.getSessionXaID(), mysqlCon);
                session.setXaState(TxState.TX_COMMIT_FAILED_STATE);
                if (decrementCountBy(1)) {
                    cleanAndFeedback(false);
                }
            }
        }
    }

    protected void setResponseTime(boolean isSuccess) {
    }

    protected void nextParse() {
        if (this.isFail() && session.getXaState() != TxState.TX_PREPARE_UNCONNECT_STATE) {
            session.getSource().setTxInterrupt(error);
            session.getSource().write(sendData);
            LOGGER.info("nextParse failed:" + error);
        } else {
            commit();
        }
    }

    private void cleanAndFeedback(boolean isSuccess) {
        if (session.getXaState() == TxState.TX_INITIALIZE_STATE) { // clear all resources
            XAStateLog.saveXARecoveryLog(session.getSessionXaID(), TxState.TX_COMMITTED_STATE);
            session.cancelableStatusSet(NonBlockingSession.CANCEL_STATUS_INIT);
            session.clearResources(false);
            if (session.closed()) {
                return;
            }
            setResponseTime(isSuccess);
            byte[] send = sendData;
            session.getSource().write(send);

            // partially committed,must commit again
        } else if (session.getXaState() == TxState.TX_COMMIT_FAILED_STATE) {
            MySQLConnection errConn = session.releaseExcept(TxState.TX_COMMIT_FAILED_STATE);
            if (errConn != null) {
                final String xaId = session.getSessionXaID();
                XAStateLog.saveXARecoveryLog(xaId, session.getXaState());
                if (++tryCommitTimes < COMMIT_TIMES) {
                    // try commit several times
                    LOGGER.warn("fail to COMMIT xa transaction " + xaId + " at the " + tryCommitTimes + "th time!");
                    XaDelayProvider.beforeInnerRetry(tryCommitTimes, xaId);
                    commit();
                } else {
                    // close this session ,add to schedule job
                    session.getSource().close("COMMIT FAILED but it will try to COMMIT repeatedly in background until it is success!");
                    // kill xa or retry to commit xa in background
                    final int count = DbleServer.getInstance().getConfig().getSystem().getXaRetryCount();
                    if (!session.isRetryXa()) {
                        String warnStr = "kill xa session by manager cmd!";
                        LOGGER.warn(warnStr);
                        session.forceClose(warnStr);
                    } else if (count == 0 || ++backgroundCommitTimes <= count) {
                        String warnStr = "fail to COMMIT xa transaction " + xaId + " at the " + backgroundCommitTimes + "th time in background!" ;
                        LOGGER.warn(warnStr);
                        AlertUtil.alertSelf(AlarmCode.XA_BACKGROUND_RETRY_FAIL, Alert.AlertLevel.WARN, warnStr, AlertUtil.genSingleLabel("XA_ID", xaId));

                        XaDelayProvider.beforeAddXaToQueue(count, xaId);
                        DbleServer.getInstance().getXaSessionCheck().addCommitSession(session);
                        XaDelayProvider.afterAddXaToQueue(count, xaId);
                    }
                }
            } else {
                XAStateLog.saveXARecoveryLog(session.getSessionXaID(), TxState.TX_COMMITTED_STATE);
                session.setXaState(TxState.TX_INITIALIZE_STATE);
                session.cancelableStatusSet(NonBlockingSession.CANCEL_STATUS_INIT);
                byte[] toSend = OkPacket.OK;
                session.clearResources(false);
                AlertUtil.alertSelfResolve(AlarmCode.XA_BACKGROUND_RETRY_FAIL, Alert.AlertLevel.WARN, AlertUtil.genSingleLabel("XA_ID", session.getSessionXaID()));
                // remove session in background
                DbleServer.getInstance().getXaSessionCheck().getCommittingSession().remove(session.getSource().getId());
                if (!session.closed()) {
                    setResponseTime(isSuccess);
                    session.getSource().write(toSend);
                }
            }

            // need to rollback;
        } else {
            XAStateLog.saveXARecoveryLog(session.getSessionXaID(), session.getXaState());
            setResponseTime(isSuccess);
            session.getSource().write(sendData);
            LOGGER.info("cleanAndFeedback:" + error);

        }
    }


    public void debugCommitDelay() {
        try {
            if (LOGGER.isDebugEnabled()) {
                long delayTime = 0;
                String xaStatus = "";
                //before the prepare command
                if (session.getXaState() == TxState.TX_ENDED_STATE) {
                    String prepareDelayTime = System.getProperty("PREPARE_DELAY");
                    delayTime = prepareDelayTime == null ? 0 : Long.parseLong(prepareDelayTime) * 1000;
                    xaStatus = "'XA PREPARED'";
                } else if (session.getXaState() == TxState.TX_PREPARED_STATE) {
                    String commitDelayTime = System.getProperty("COMMIT_DELAY");
                    delayTime = commitDelayTime == null ? 0 : Long.parseLong(commitDelayTime) * 1000;
                    xaStatus = "'XA COMMIT'";
                }
                //if using the debug log & using the jvm xa delay properties action will be delay by properties
                if (delayTime > 0) {
                    LOGGER.debug("before xa " + xaStatus + " sleep time = " + delayTime);
                    Thread.sleep(delayTime);
                    LOGGER.debug("before xa " + xaStatus + " sleep finished " + delayTime);
                }
            }
        } catch (Exception e) {
            LOGGER.debug("before xa commit sleep error ");
        }

    }

    private void waitUntilSendFinish() {
        this.lockForErrorHandle.lock();
        try {
            if (!this.sendFinishedFlag) {
                this.sendFinished.await();
            }
        } catch (Exception e) {
            LOGGER.info("back Response is closed by thread interrupted");
        } finally {
            lockForErrorHandle.unlock();
        }
    }

}
