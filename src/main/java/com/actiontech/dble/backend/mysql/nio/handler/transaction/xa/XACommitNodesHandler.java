/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.transaction.xa;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.AbstractCommitNodesHandler;
import com.actiontech.dble.backend.mysql.xa.CoordinatorLogEntry;
import com.actiontech.dble.backend.mysql.xa.ParticipantLogEntry;
import com.actiontech.dble.backend.mysql.xa.TxState;
import com.actiontech.dble.backend.mysql.xa.XAStateLog;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.util.StringUtil;

public class XACommitNodesHandler extends AbstractCommitNodesHandler {
    private static final int COMMIT_TIMES = 5;
    private int tryCommitTimes = 0;
    private ParticipantLogEntry[] participantLogEntry = null;
    protected byte[] sendData = OkPacket.OK;

    public XACommitNodesHandler(NonBlockingSession session) {
        super(session);
    }

    @Override
    public void clearResources() {
        tryCommitTimes = 0;
        participantLogEntry = null;
        sendData = OkPacket.OK;
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
            XAStateLog.initRecoverylog(session.getSessionXaID(), position, mysqlCon);
            endPhase(mysqlCon);
        } else if (state == TxState.TX_ENDED_STATE) {
            if (position == 0) {
                if (!XAStateLog.saveXARecoverylog(session.getSessionXaID(), TxState.TX_PREPARING_STATE)) {
                    String errMsg = "saveXARecoverylog error, the stage is TX_PREPARING_STATE";
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
                if (!XAStateLog.saveXARecoverylog(session.getSessionXaID(), TxState.TX_COMMITING_STATE)) {
                    String errMsg = "saveXARecoverylog error, the stage is TX_COMMITING_STATE";
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
                XAStateLog.saveXARecoverylog(session.getSessionXaID(), TxState.TX_COMMIT_FAILED_STATE);
            }
            commitPhase(mysqlCon);
        }
        return true;
    }

    private byte[] makeErrorPacket(String errMsg) {
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.setErrno(ErrorCode.ER_UNKNOWN_ERROR);
        errPacket.setMessage(StringUtil.encode(errMsg, session.getSource().getCharset().getResults()));
        return errPacket.toBytes();
    }

    private void endPhase(MySQLConnection mysqlCon) {
        String xaTxId = mysqlCon.getConnXID(session);
        mysqlCon.execCmd("XA END " + xaTxId);
    }

    private void preparePhase(MySQLConnection mysqlCon) {
        String xaTxId = mysqlCon.getConnXID(session);
        mysqlCon.execCmd("XA PREPARE " + xaTxId);
    }

    private void commitPhase(MySQLConnection mysqlCon) {
        if (session.getXaState() == TxState.TX_COMMIT_FAILED_STATE) {
            MySQLConnection newConn = session.freshConn(mysqlCon, this);
            if (!newConn.equals(mysqlCon)) {
                mysqlCon = newConn;
            } else if (decrementCountBy(1)) {
                cleanAndFeedback();
                return;
            }
        }
        String xaTxId = mysqlCon.getConnXID(session);
        mysqlCon.execCmd("XA COMMIT " + xaTxId);
    }

    @Override
    public void okResponse(byte[] ok, BackendConnection conn) {
        MySQLConnection mysqlCon = (MySQLConnection) conn;
        TxState state = mysqlCon.getXaStatus();
        if (state == TxState.TX_STARTED_STATE) {
            mysqlCon.setXaStatus(TxState.TX_ENDED_STATE);
            XAStateLog.saveXARecoverylog(session.getSessionXaID(), mysqlCon);
            if (decrementCountBy(1)) {
                session.setXaState(TxState.TX_ENDED_STATE);
                nextParse();
            }
        } else if (state == TxState.TX_ENDED_STATE) {
            //PREPARE OK
            mysqlCon.setXaStatus(TxState.TX_PREPARED_STATE);
            XAStateLog.saveXARecoverylog(session.getSessionXaID(), mysqlCon);
            if (decrementCountBy(1)) {
                if (session.getXaState() == TxState.TX_ENDED_STATE) {
                    session.setXaState(TxState.TX_PREPARED_STATE);
                }
                nextParse();
            }
        } else if (state == TxState.TX_COMMIT_FAILED_STATE || state == TxState.TX_PREPARED_STATE) {
            //COMMIT OK
            // XA reset status now
            mysqlCon.setXaStatus(TxState.TX_COMMITED_STATE);
            XAStateLog.saveXARecoverylog(session.getSessionXaID(), mysqlCon);
            mysqlCon.setXaStatus(TxState.TX_INITIALIZE_STATE);
            if (decrementCountBy(1)) {
                if (session.getXaState() == TxState.TX_PREPARED_STATE) {
                    session.setXaState(TxState.TX_INITIALIZE_STATE);
                }
                cleanAndFeedback();
            }
            // LOGGER.error("Wrong XA status flag!");
        }
    }

    @Override
    public void errorResponse(byte[] err, BackendConnection conn) {
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.read(err);
        String errmsg = new String(errPacket.getMessage());
        this.setFail(errmsg);
        sendData = makeErrorPacket(errmsg);
        if (conn instanceof MySQLConnection) {
            MySQLConnection mysqlCon = (MySQLConnection) conn;
            if (mysqlCon.getXaStatus() == TxState.TX_STARTED_STATE) {
                mysqlCon.quit();
                mysqlCon.setXaStatus(TxState.TX_CONN_QUIT);
                XAStateLog.saveXARecoverylog(session.getSessionXaID(), mysqlCon);
                if (decrementCountBy(1)) {
                    session.setXaState(TxState.TX_ENDED_STATE);
                    nextParse();
                }

                // 'xa prepare' error
            } else if (mysqlCon.getXaStatus() == TxState.TX_ENDED_STATE) {
                mysqlCon.quit();
                mysqlCon.setXaStatus(TxState.TX_CONN_QUIT);
                XAStateLog.saveXARecoverylog(session.getSessionXaID(), mysqlCon);
                if (decrementCountBy(1)) {
                    if (session.getXaState() == TxState.TX_ENDED_STATE) {
                        session.setXaState(TxState.TX_PREPARED_STATE);
                    }
                    nextParse();
                }

                // 'xa commit' err
            } else if (mysqlCon.getXaStatus() == TxState.TX_COMMIT_FAILED_STATE || mysqlCon.getXaStatus() == TxState.TX_PREPARED_STATE) { //TODO:service degradation?
                mysqlCon.setXaStatus(TxState.TX_COMMIT_FAILED_STATE);
                XAStateLog.saveXARecoverylog(session.getSessionXaID(), mysqlCon);
                session.setXaState(TxState.TX_COMMIT_FAILED_STATE);
                if (decrementCountBy(1)) {
                    cleanAndFeedback();
                }
            }
        }
    }

    @Override
    public void connectionError(Throwable e, BackendConnection conn) {
        LOGGER.warn("backend connect", e);
        String errmsg = new String(StringUtil.encode(e.getMessage(), session.getSource().getCharset().getResults()));
        this.setFail(errmsg);
        sendData = makeErrorPacket(errmsg);
        if (conn instanceof MySQLConnection) {
            MySQLConnection mysqlCon = (MySQLConnection) conn;
            if (mysqlCon.getXaStatus() == TxState.TX_STARTED_STATE) {
                mysqlCon.quit();
                mysqlCon.setXaStatus(TxState.TX_CONN_QUIT);
                XAStateLog.saveXARecoverylog(session.getSessionXaID(), mysqlCon);
                if (decrementCountBy(1)) {
                    session.setXaState(TxState.TX_ENDED_STATE);
                    nextParse();
                }

                // 'xa prepare' connectionError
            } else if (mysqlCon.getXaStatus() == TxState.TX_ENDED_STATE) {
                mysqlCon.setXaStatus(TxState.TX_PREPARE_UNCONNECT_STATE);
                XAStateLog.saveXARecoverylog(session.getSessionXaID(), mysqlCon);
                session.setXaState(TxState.TX_PREPARE_UNCONNECT_STATE);
                if (decrementCountBy(1)) {
                    nextParse();
                }

                // 'xa commit' connectionError
            } else if (mysqlCon.getXaStatus() == TxState.TX_COMMIT_FAILED_STATE || mysqlCon.getXaStatus() == TxState.TX_PREPARED_STATE) { //TODO:service degradation?
                mysqlCon.setXaStatus(TxState.TX_COMMIT_FAILED_STATE);
                XAStateLog.saveXARecoverylog(session.getSessionXaID(), mysqlCon);
                session.setXaState(TxState.TX_COMMIT_FAILED_STATE);
                if (decrementCountBy(1)) {
                    cleanAndFeedback();
                }
            }
        }
    }

    @Override
    public void connectionClose(BackendConnection conn, String reason) {
        this.setFail(reason);
        sendData = makeErrorPacket(reason);
        if (conn instanceof MySQLConnection) {
            MySQLConnection mysqlCon = (MySQLConnection) conn;
            if (mysqlCon.getXaStatus() == TxState.TX_STARTED_STATE) {
                mysqlCon.quit();
                mysqlCon.setXaStatus(TxState.TX_CONN_QUIT);
                XAStateLog.saveXARecoverylog(session.getSessionXaID(), mysqlCon);
                if (decrementCountBy(1)) {
                    session.setXaState(TxState.TX_ENDED_STATE);
                    nextParse();
                }

                //  'xa prepare' connectionClose,conn has quit
            } else if (mysqlCon.getXaStatus() == TxState.TX_ENDED_STATE) {
                mysqlCon.setXaStatus(TxState.TX_PREPARE_UNCONNECT_STATE);
                XAStateLog.saveXARecoverylog(session.getSessionXaID(), mysqlCon);
                session.setXaState(TxState.TX_PREPARE_UNCONNECT_STATE);
                if (decrementCountBy(1)) {
                    nextParse();
                }

                // 'xa commit' connectionClose
            } else if (mysqlCon.getXaStatus() == TxState.TX_COMMIT_FAILED_STATE || mysqlCon.getXaStatus() == TxState.TX_PREPARED_STATE) { //TODO:service degradation?
                mysqlCon.setXaStatus(TxState.TX_COMMIT_FAILED_STATE);
                XAStateLog.saveXARecoverylog(session.getSessionXaID(), mysqlCon);
                session.setXaState(TxState.TX_COMMIT_FAILED_STATE);
                if (decrementCountBy(1)) {
                    cleanAndFeedback();
                }
            }
        }
    }

    protected void nextParse() {
        if (this.isFail()) {
            session.getSource().setTxInterrupt(error);
            session.getSource().write(sendData);
            LOGGER.warn("nextParse failed:" + error);
        } else {
            commit();
        }
    }

    private void cleanAndFeedback() {
        if (session.getXaState() == TxState.TX_INITIALIZE_STATE) { // clear all resources
            XAStateLog.saveXARecoverylog(session.getSessionXaID(), TxState.TX_COMMITED_STATE);
            session.cancelableStatusSet(NonBlockingSession.CANCEL_STATUS_INIT);
            byte[] send = sendData;
            session.clearResources(false);
            if (session.closed()) {
                return;
            }
            session.getSource().write(send);

            // partitionly commited,must commit again
        } else if (session.getXaState() == TxState.TX_COMMIT_FAILED_STATE) {
            MySQLConnection errConn = session.releaseExcept(TxState.TX_COMMIT_FAILED_STATE);
            if (errConn != null) {
                XAStateLog.saveXARecoverylog(session.getSessionXaID(), session.getXaState());
                if (++tryCommitTimes < COMMIT_TIMES) {
                    // try commit several times
                    commit();
                } else {
                    // close this session ,add to schedule job
                    session.getSource().close("COMMIT FAILED but it will try to COMMIT repeatedly in backend until it is success!");
                    DbleServer.getInstance().getXaSessionCheck().addCommitSession(session);
                }
            } else {
                XAStateLog.saveXARecoverylog(session.getSessionXaID(), TxState.TX_COMMITED_STATE);
                session.setXaState(TxState.TX_INITIALIZE_STATE);
                session.cancelableStatusSet(NonBlockingSession.CANCEL_STATUS_INIT);
                byte[] toSend = sendData;
                session.clearResources(false);
                if (!session.closed()) {
                    session.getSource().write(toSend);
                }
            }

            // need to rollback;
        } else {
            XAStateLog.saveXARecoverylog(session.getSessionXaID(), session.getXaState());
            session.getSource().write(sendData);
            LOGGER.warn("cleanAndFeedback:" + error);

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
}
