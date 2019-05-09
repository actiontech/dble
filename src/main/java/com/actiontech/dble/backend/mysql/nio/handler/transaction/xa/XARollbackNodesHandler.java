/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.backend.mysql.nio.handler.transaction.xa;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.AbstractRollbackNodesHandler;
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

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author mycat
 */
public class XARollbackNodesHandler extends AbstractRollbackNodesHandler {
    private static final int ROLLBACK_TIMES = 5;
    private int tryRollbackTimes = 0;
    private int backgroundRollbackTimes = 0;
    private ParticipantLogEntry[] participantLogEntry = null;
    byte[] sendData = OkPacket.OK;

    private volatile boolean sendFinishedFlag = false;
    private Lock lockForErrorHandle = new ReentrantLock();
    private Condition sendFinished = lockForErrorHandle.newCondition();

    public XARollbackNodesHandler(NonBlockingSession session) {
        super(session);
    }

    @Override
    public void clearResources() {
        tryRollbackTimes = 0;
        backgroundRollbackTimes = 0;
        participantLogEntry = null;
        sendData = OkPacket.OK;
    }

    public void rollback() {
        final int initCount = session.getTargetCount();
        lock.lock();
        try {
            reset(initCount);
        } finally {
            lock.unlock();
        }
        int position = 0;
        //get session's lock before sending rollback(in fact, after ended)
        //then the XA transaction will be not killed. if killed ,then we will not rollback
        if (session.getXaState() != null &&
                session.getXaState() == TxState.TX_ENDED_STATE) {
            if (!session.cancelableStatusSet(NonBlockingSession.CANCEL_STATUS_COMMITTING)) {
                return;
            }
        }

        try {
            sendFinishedFlag = false;
            for (final RouteResultsetNode node : session.getTargetKeys()) {
                final BackendConnection conn = session.getTarget(node);
                conn.setResponseHandler(this);
                if (!executeRollback((MySQLConnection) conn, position++)) {
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


    protected void setResponseTime(boolean isSuccess) {
    }

    private boolean executeRollback(MySQLConnection mysqlCon, int position) {
        if (position == 0 && participantLogEntry != null) {
            XAStateLog.saveXARecoveryLog(session.getSessionXaID(), session.getXaState());
        }
        if (session.getXaState() == TxState.TX_STARTED_STATE) {
            if (participantLogEntry == null) {
                participantLogEntry = new ParticipantLogEntry[nodeCount];
                CoordinatorLogEntry coordinatorLogEntry = new CoordinatorLogEntry(session.getSessionXaID(), participantLogEntry, session.getXaState());
                XAStateLog.flushMemoryRepository(session.getSessionXaID(), coordinatorLogEntry);
            }
            XAStateLog.initRecoveryLog(session.getSessionXaID(), position, mysqlCon);
            if (mysqlCon.isClosed()) {
                mysqlCon.setXaStatus(TxState.TX_CONN_QUIT);
            }
            endPhase(mysqlCon);

        } else if (session.getXaState() == TxState.TX_PREPARED_STATE) {
            if (position == 0) {
                if (!XAStateLog.saveXARecoveryLog(session.getSessionXaID(), TxState.TX_ROLLBACKING_STATE)) {
                    this.setFail("saveXARecoveryLog error, the stage is TX_ROLLBACKING_STATE");
                    cleanAndFeedback();
                    return false;
                }
                this.debugRollbackDelay();
            }
            rollbackPhase(mysqlCon);

        } else if (session.getXaState() == TxState.TX_ROLLBACK_FAILED_STATE || session.getXaState() == TxState.TX_PREPARE_UNCONNECT_STATE) {
            rollbackPhase(mysqlCon);

        } else if (session.getXaState() == TxState.TX_ENDED_STATE) {
            if (position == 0) {
                this.debugRollbackDelay();
            }
            rollbackPhase(mysqlCon);
        }
        return true;
    }

    private void endPhase(MySQLConnection mysqlCon) {
        if (mysqlCon.getXaStatus() == TxState.TX_STARTED_STATE) {
            RouteResultsetNode rrn = (RouteResultsetNode) mysqlCon.getAttachment();
            String xaTxId = mysqlCon.getConnXID(session, rrn.getMultiplexNum().longValue());
            XaDelayProvider.delayBeforeXaEnd(rrn.getName(), xaTxId);
            mysqlCon.execCmd("XA END " + xaTxId + ";");

        } else if (mysqlCon.getXaStatus() == TxState.TX_CONN_QUIT) {
            if (decrementCountBy(1)) {
                session.setXaState(TxState.TX_ENDED_STATE);
                rollback();
            }
        }

    }

    private void rollbackPhase(MySQLConnection mysqlCon) {
        if (mysqlCon.getXaStatus() == TxState.TX_ROLLBACK_FAILED_STATE || mysqlCon.getXaStatus() == TxState.TX_PREPARE_UNCONNECT_STATE) {
            MySQLConnection newConn = session.freshConn(mysqlCon, this);
            if (!newConn.equals(mysqlCon)) {
                mysqlCon = newConn;
                RouteResultsetNode rrn = (RouteResultsetNode) mysqlCon.getAttachment();
                String xaTxId = mysqlCon.getConnXID(session, rrn.getMultiplexNum().longValue());
                XaDelayProvider.delayBeforeXaRollback(rrn.getName(), xaTxId);
                mysqlCon.execCmd("XA ROLLBACK " + xaTxId + ";");
            } else if (decrementCountBy(1)) {
                cleanAndFeedback();
            }

        } else if (mysqlCon.getXaStatus() == TxState.TX_ENDED_STATE || mysqlCon.getXaStatus() == TxState.TX_PREPARED_STATE) {
            RouteResultsetNode rrn = (RouteResultsetNode) mysqlCon.getAttachment();
            String xaTxId = mysqlCon.getConnXID(session, rrn.getMultiplexNum().longValue());
            XaDelayProvider.delayBeforeXaRollback(rrn.getName(), xaTxId);
            mysqlCon.execCmd("XA ROLLBACK " + xaTxId + ";");

        } else if (mysqlCon.getXaStatus() == TxState.TX_CONN_QUIT || mysqlCon.getXaStatus() == TxState.TX_ROLLBACKED_STATE) {
            if (decrementCountBy(1)) {
                cleanAndFeedback();
            }
        }
    }

    @Override
    public void okResponse(byte[] ok, BackendConnection conn) {
        this.waitUntilSendFinish();
        if (conn instanceof MySQLConnection) {
            MySQLConnection mysqlCon = (MySQLConnection) conn;
            if (mysqlCon.getXaStatus() == TxState.TX_STARTED_STATE) {
                mysqlCon.setXaStatus(TxState.TX_ENDED_STATE);
                XAStateLog.saveXARecoveryLog(session.getSessionXaID(), mysqlCon);
                if (decrementCountBy(1)) {
                    session.setXaState(TxState.TX_ENDED_STATE);
                    rollback();
                }
                // 'xa rollback' ok without prepared
            } else if (mysqlCon.getXaStatus() == TxState.TX_ENDED_STATE) {
                mysqlCon.setXaStatus(TxState.TX_ROLLBACKED_STATE);
                XAStateLog.saveXARecoveryLog(session.getSessionXaID(), mysqlCon);
                mysqlCon.setXaStatus(TxState.TX_INITIALIZE_STATE);
                if (decrementCountBy(1)) {
                    session.setXaState(TxState.TX_INITIALIZE_STATE);
                    cleanAndFeedback();
                }
                // 'xa rollback' ok
            } else if (mysqlCon.getXaStatus() == TxState.TX_PREPARED_STATE ||
                    mysqlCon.getXaStatus() == TxState.TX_PREPARE_UNCONNECT_STATE ||
                    mysqlCon.getXaStatus() == TxState.TX_ROLLBACK_FAILED_STATE) {
                // we don't know if the conn prepared or not
                mysqlCon.setXaStatus(TxState.TX_ROLLBACKED_STATE);
                XAStateLog.saveXARecoveryLog(session.getSessionXaID(), mysqlCon);
                mysqlCon.setXaStatus(TxState.TX_INITIALIZE_STATE);
                if (decrementCountBy(1)) {
                    if (session.getXaState() == TxState.TX_PREPARED_STATE) {
                        session.setXaState(TxState.TX_INITIALIZE_STATE);
                    }
                    cleanAndFeedback();
                }
            } else {
                LOGGER.info("Wrong XA status flag!");
            }
        }
    }

    @Override
    public void errorResponse(byte[] err, BackendConnection conn) {
        this.waitUntilSendFinish();
        if (conn instanceof MySQLConnection) {
            MySQLConnection mysqlCon = (MySQLConnection) conn;
            if (mysqlCon.getXaStatus() == TxState.TX_STARTED_STATE) {
                mysqlCon.quit();
                mysqlCon.setXaStatus(TxState.TX_ROLLBACKED_STATE);
                XAStateLog.saveXARecoveryLog(session.getSessionXaID(), mysqlCon);
                if (decrementCountBy(1)) {
                    session.setXaState(TxState.TX_ENDED_STATE);
                    rollback();
                }

                // 'xa rollback' ok without prepared
            } else if (mysqlCon.getXaStatus() == TxState.TX_ENDED_STATE) {
                mysqlCon.quit();
                mysqlCon.setXaStatus(TxState.TX_ROLLBACKED_STATE);
                XAStateLog.saveXARecoveryLog(session.getSessionXaID(), mysqlCon);
                if (decrementCountBy(1)) {
                    session.setXaState(TxState.TX_INITIALIZE_STATE);
                    cleanAndFeedback();
                }

                // 'xa rollback' err
            } else if (mysqlCon.getXaStatus() == TxState.TX_ROLLBACK_FAILED_STATE || mysqlCon.getXaStatus() == TxState.TX_PREPARED_STATE) {
                mysqlCon.setXaStatus(TxState.TX_ROLLBACK_FAILED_STATE);
                mysqlCon.quit();
                XAStateLog.saveXARecoveryLog(session.getSessionXaID(), mysqlCon);
                session.setXaState(TxState.TX_ROLLBACK_FAILED_STATE);
                if (decrementCountBy(1)) {
                    cleanAndFeedback();
                }

                // we don't know if the conn prepared or not
            } else if (mysqlCon.getXaStatus() == TxState.TX_PREPARE_UNCONNECT_STATE) {
                ErrorPacket errPacket = new ErrorPacket();
                errPacket.read(err);
                if (errPacket.getErrNo() == ErrorCode.ER_XAER_NOTA) {
                    //ERROR 1397 (XAE04): XAER_NOTA: Unknown XID, not prepared
                    mysqlCon.setXaStatus(TxState.TX_ROLLBACKED_STATE);
                    XAStateLog.saveXARecoveryLog(session.getSessionXaID(), mysqlCon);
                    mysqlCon.setXaStatus(TxState.TX_INITIALIZE_STATE);
                    if (decrementCountBy(1)) {
                        if (session.getXaState() == TxState.TX_PREPARED_STATE) {
                            session.setXaState(TxState.TX_INITIALIZE_STATE);
                        }
                        cleanAndFeedback();
                    }
                } else {
                    session.setXaState(TxState.TX_ROLLBACK_FAILED_STATE);
                    XAStateLog.saveXARecoveryLog(session.getSessionXaID(), mysqlCon);
                    if (decrementCountBy(1)) {
                        cleanAndFeedback();
                    }
                }
            } else {
                LOGGER.info("Wrong XA status flag!");
            }
        }
    }

    @Override
    public void connectionError(Throwable e, BackendConnection conn) {
        this.waitUntilSendFinish();
        if (conn instanceof MySQLConnection) {
            MySQLConnection mysqlCon = (MySQLConnection) conn;
            if (mysqlCon.getXaStatus() == TxState.TX_STARTED_STATE) {
                mysqlCon.quit();
                mysqlCon.setXaStatus(TxState.TX_ROLLBACKED_STATE);
                XAStateLog.saveXARecoveryLog(session.getSessionXaID(), mysqlCon);
                if (decrementCountBy(1)) {
                    session.setXaState(TxState.TX_ENDED_STATE);
                    rollback();
                }

                // 'xa rollback' ok without prepared
            } else if (mysqlCon.getXaStatus() == TxState.TX_ENDED_STATE) {
                mysqlCon.quit();
                mysqlCon.setXaStatus(TxState.TX_ROLLBACKED_STATE);
                XAStateLog.saveXARecoveryLog(session.getSessionXaID(), mysqlCon);
                if (decrementCountBy(1)) {
                    session.setXaState(TxState.TX_INITIALIZE_STATE);
                    cleanAndFeedback();
                }

                // 'xa rollback' err
            } else if (mysqlCon.getXaStatus() == TxState.TX_PREPARED_STATE) {
                mysqlCon.setXaStatus(TxState.TX_ROLLBACK_FAILED_STATE);
                mysqlCon.quit();
                XAStateLog.saveXARecoveryLog(session.getSessionXaID(), mysqlCon);
                session.setXaState(TxState.TX_ROLLBACK_FAILED_STATE);
                if (decrementCountBy(1)) {
                    cleanAndFeedback();
                }
                // we don't know if the conn prepared or not
            } else if (mysqlCon.getXaStatus() == TxState.TX_PREPARE_UNCONNECT_STATE) {
                session.setXaState(TxState.TX_ROLLBACK_FAILED_STATE);
                XAStateLog.saveXARecoveryLog(session.getSessionXaID(), mysqlCon);
                if (decrementCountBy(1)) {
                    cleanAndFeedback();
                }
            } else {
                LOGGER.info("Wrong XA status flag!");
            }
        }
    }

    @Override
    public void connectionClose(BackendConnection conn, String reason) {
        this.waitUntilSendFinish();
        this.setFail(reason);
        if (conn instanceof MySQLConnection) {
            MySQLConnection mysqlCon = (MySQLConnection) conn;
            if (mysqlCon.getXaStatus() == TxState.TX_STARTED_STATE) {
                mysqlCon.quit();
                mysqlCon.setXaStatus(TxState.TX_ROLLBACKED_STATE);
                XAStateLog.saveXARecoveryLog(session.getSessionXaID(), mysqlCon);
                if (decrementCountBy(1)) {
                    session.setXaState(TxState.TX_ENDED_STATE);
                    rollback();
                }
                // 'xa rollback' ok without prepared
            } else if (mysqlCon.getXaStatus() == TxState.TX_ENDED_STATE) {
                mysqlCon.quit();
                mysqlCon.setXaStatus(TxState.TX_ROLLBACKED_STATE);
                XAStateLog.saveXARecoveryLog(session.getSessionXaID(), mysqlCon);
                if (decrementCountBy(1)) {
                    session.setXaState(TxState.TX_INITIALIZE_STATE);
                    cleanAndFeedback();
                }
                // 'xa rollback' err
            } else if (mysqlCon.getXaStatus() == TxState.TX_PREPARED_STATE) {
                mysqlCon.setXaStatus(TxState.TX_ROLLBACK_FAILED_STATE);
                mysqlCon.quit();
                XAStateLog.saveXARecoveryLog(session.getSessionXaID(), mysqlCon);
                session.setXaState(TxState.TX_ROLLBACK_FAILED_STATE);
                if (decrementCountBy(1)) {
                    cleanAndFeedback();
                }
            } else {
                LOGGER.info("Wrong XA status flag!");
            }
        }
    }

    private void cleanAndFeedback() {
        if (session.getXaState() == TxState.TX_INITIALIZE_STATE) { // clear all resources
            XAStateLog.saveXARecoveryLog(session.getSessionXaID(), TxState.TX_ROLLBACKED_STATE);
            session.cancelableStatusSet(NonBlockingSession.CANCEL_STATUS_INIT);
            session.clearResources(false);
            if (session.closed()) {
                return;
            }
            setResponseTime(false);
            byte[] send = sendData;
            session.getSource().write(send);

            //partially committed,must commit again
        } else if (session.getXaState() == TxState.TX_ROLLBACK_FAILED_STATE || session.getXaState() == TxState.TX_PREPARED_STATE ||
                session.getXaState() == TxState.TX_PREPARE_UNCONNECT_STATE) {
            MySQLConnection errConn = session.releaseExcept(session.getXaState());
            if (errConn != null) {
                final String xaId = session.getSessionXaID();
                XAStateLog.saveXARecoveryLog(xaId, session.getXaState());
                if (++tryRollbackTimes < ROLLBACK_TIMES) {
                    // try rollback several times
                    LOGGER.warn("fail to ROLLBACK xa transaction " + xaId + " at the " + tryRollbackTimes + "th time!");
                    XaDelayProvider.beforeInnerRetry(tryRollbackTimes, xaId);
                    rollback();
                } else {
                    StringBuilder closeReason = new StringBuilder("ROLLBACK FAILED but it will try to ROLLBACK repeatedly in background until it is success!");
                    if (error != null) {
                        closeReason.append(", the ERROR is ");
                        closeReason.append(error);
                    }
                    // close the session ,add to schedule job
                    session.getSource().close(closeReason.toString());
                    // kill xa or retry to rollback xa in background
                    final int count = DbleServer.getInstance().getConfig().getSystem().getXaRetryCount();
                    if (!session.isRetryXa()) {
                        String warnStr = "kill xa session by manager cmd!";
                        LOGGER.warn(warnStr);
                        session.forceClose(warnStr);
                    } else if (count == 0 || ++backgroundRollbackTimes <= count) {
                        String warnStr = "fail to ROLLBACK xa transaction " + xaId + " at the " + backgroundRollbackTimes + "th time in background!" ;
                        LOGGER.warn(warnStr);
                        AlertUtil.alertSelf(AlarmCode.XA_BACKGROUND_RETRY_FAIL, Alert.AlertLevel.WARN, warnStr, AlertUtil.genSingleLabel("XA_ID", xaId));

                        XaDelayProvider.beforeAddXaToQueue(count, xaId);
                        DbleServer.getInstance().getXaSessionCheck().addRollbackSession(session);
                        XaDelayProvider.afterAddXaToQueue(count, xaId);
                    }
                }
            } else {
                XAStateLog.saveXARecoveryLog(session.getSessionXaID(), TxState.TX_ROLLBACKED_STATE);
                session.setXaState(TxState.TX_INITIALIZE_STATE);
                byte[] toSend = OkPacket.OK;
                session.clearResources(false);
                AlertUtil.alertSelfResolve(AlarmCode.XA_BACKGROUND_RETRY_FAIL, Alert.AlertLevel.WARN, AlertUtil.genSingleLabel("XA_ID", session.getSessionXaID()));
                // remove session in background
                DbleServer.getInstance().getXaSessionCheck().getRollbackingSession().remove(session.getSource().getId());
                if (!session.closed()) {
                    setResponseTime(false);
                    session.getSource().write(toSend);
                }
            }

            // rollback success,but closed coon must remove
        } else {
            removeQuitConn();
            XAStateLog.saveXARecoveryLog(session.getSessionXaID(), TxState.TX_ROLLBACKED_STATE);
            session.setXaState(TxState.TX_INITIALIZE_STATE);
            session.clearResources(false);
            if (session.closed()) {
                return;
            }
            setResponseTime(false);
            session.getSource().write(sendData);

        }
    }

    private void removeQuitConn() {
        for (final RouteResultsetNode node : session.getTargetKeys()) {
            final MySQLConnection mysqlCon = (MySQLConnection) session.getTarget(node);
            if (mysqlCon.getXaStatus() != TxState.TX_CONN_QUIT && mysqlCon.getXaStatus() != TxState.TX_ROLLBACKED_STATE) {
                session.getTargetMap().remove(node);
            }
        }
    }


    private void debugRollbackDelay() {
        try {
            if (LOGGER.isDebugEnabled()) {
                String rollbackDelayTime = System.getProperty("ROLLBACK_DELAY");
                long delayTime = rollbackDelayTime == null ? 0 : Long.parseLong(rollbackDelayTime) * 1000;
                LOGGER.debug("before xa rollback sleep time = " + delayTime);
                Thread.sleep(delayTime);
                LOGGER.debug("before xa rollback sleep finished " + delayTime);
            }
        } catch (Exception e) {
            LOGGER.debug("before xa rollback  sleep error ");
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
