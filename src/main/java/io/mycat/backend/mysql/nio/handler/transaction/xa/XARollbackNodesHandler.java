/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese
 * opensource volunteers. you can redistribute it and/or modify it under the
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Any questions about this component can be directed to it's project Web address
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.backend.mysql.nio.handler.transaction.xa;

import io.mycat.MycatServer;
import io.mycat.backend.BackendConnection;
import io.mycat.backend.mysql.nio.MySQLConnection;
import io.mycat.backend.mysql.nio.handler.transaction.AbstractRollbackNodesHandler;
import io.mycat.backend.mysql.xa.CoordinatorLogEntry;
import io.mycat.backend.mysql.xa.ParticipantLogEntry;
import io.mycat.backend.mysql.xa.TxState;
import io.mycat.backend.mysql.xa.XAStateLog;
import io.mycat.config.ErrorCode;
import io.mycat.config.model.SystemConfig;
import io.mycat.net.mysql.ErrorPacket;
import io.mycat.net.mysql.OkPacket;
import io.mycat.route.RouteResultsetNode;
import io.mycat.server.NonBlockingSession;

/**
 * @author mycat
 */
public class XARollbackNodesHandler extends AbstractRollbackNodesHandler {
    private static final int ROLLBACK_TIMES = 5;
    private int tryRollbackTimes = 0;
    private ParticipantLogEntry[] participantLogEntry = null;
    protected byte[] sendData = OkPacket.OK;

    public XARollbackNodesHandler(NonBlockingSession session) {
        super(session);
    }

    @Override
    public void clearResources() {
        tryRollbackTimes = 0;
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
        // 执行
        int position = 0;
        //在发起真正的rollback之前就获取到session级别的锁
        //当执行END的时候我们就可以认为这个状态处于一种不定的状态
        //不再允许XA事务被kill,如果事务已经被kill那么我们不再执行commit
        if (session.getXaState() != null &&
                session.getXaState() == TxState.TX_ENDED_STATE) {
            if (!session.cancelableStatusSet(NonBlockingSession.CANCEL_STATUS_COMMITING)) {
                return;
            }
        }

        for (final RouteResultsetNode node : session.getTargetKeys()) {
            final BackendConnection conn = session.getTarget(node);
            conn.setResponseHandler(this);
            if (!executeRollback((MySQLConnection) conn, position++)) {
                break;
            }
        }
    }

    private boolean executeRollback(MySQLConnection mysqlCon, int position) {
        if (position == 0 && participantLogEntry != null) {
            XAStateLog.saveXARecoverylog(session.getSessionXaID(), session.getXaState());
        }
        if (session.getXaState() == TxState.TX_STARTED_STATE) {
            if (participantLogEntry == null) {
                participantLogEntry = new ParticipantLogEntry[nodeCount];
                CoordinatorLogEntry coordinatorLogEntry = new CoordinatorLogEntry(session.getSessionXaID(), participantLogEntry, session.getXaState());
                XAStateLog.flushMemoryRepository(session.getSessionXaID(), coordinatorLogEntry);
            }
            XAStateLog.initRecoverylog(session.getSessionXaID(), position, mysqlCon);
            if (mysqlCon.isClosed()) {
                mysqlCon.setXaStatus(TxState.TX_CONN_QUIT);
            }
            endPhase(mysqlCon);

        } else if (session.getXaState() == TxState.TX_PREPARED_STATE) {
            if (position == 0) {
                if (!XAStateLog.saveXARecoverylog(session.getSessionXaID(), TxState.TX_ROLLBACKING_STATE)) {
                    this.setFail("saveXARecoverylog error, the stage is TX_ROLLBACKING_STATE");
                    cleanAndFeedback();
                    return false;
                }
                this.debugRollbackDelay();
            }
            rollbackPhase(mysqlCon);

        } else if (session.getXaState() == TxState.TX_ROLLBACK_FAILED_STATE || session.getXaState() == TxState.TX_PREPARE_UNCONNECT_STATE) {
            rollbackPhase(mysqlCon);

        } else if (session.getXaState() == TxState.TX_ENDED_STATE) {
            if (mysqlCon.isClosed()) {
                mysqlCon.setXaStatus(TxState.TX_CONN_QUIT);
            }
            rollbackPhase(mysqlCon);
        }
        return true;
    }

    private void endPhase(MySQLConnection mysqlCon) {
        if (mysqlCon.getXaStatus() == TxState.TX_STARTED_STATE) {
            String xaTxId = mysqlCon.getConnXID(session);
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
                String xaTxId = mysqlCon.getConnXID(session);
                mysqlCon.execCmd("XA ROLLBACK " + xaTxId + ";");
            } else if (decrementCountBy(1)) {
                cleanAndFeedback();
            }

        } else if (mysqlCon.getXaStatus() == TxState.TX_ENDED_STATE || mysqlCon.getXaStatus() == TxState.TX_PREPARED_STATE) {
            String xaTxId = mysqlCon.getConnXID(session);
            mysqlCon.execCmd("XA ROLLBACK " + xaTxId + ";");

        } else if (mysqlCon.getXaStatus() == TxState.TX_CONN_QUIT || mysqlCon.getXaStatus() == TxState.TX_ROLLBACKED_STATE) {
            if (decrementCountBy(1)) {
                cleanAndFeedback();
            }

        }
    }

    @Override
    public void okResponse(byte[] ok, BackendConnection conn) {
        if (conn instanceof MySQLConnection) {
            MySQLConnection mysqlCon = (MySQLConnection) conn;
            if (mysqlCon.getXaStatus() == TxState.TX_STARTED_STATE) {
                mysqlCon.setXaStatus(TxState.TX_ENDED_STATE);
                XAStateLog.saveXARecoverylog(session.getSessionXaID(), mysqlCon);
                if (decrementCountBy(1)) {
                    session.setXaState(TxState.TX_ENDED_STATE);
                    rollback();
                }

                // 'xa rollback' ok without prepared
            } else if (mysqlCon.getXaStatus() == TxState.TX_ENDED_STATE) {
                mysqlCon.setXaStatus(TxState.TX_ROLLBACKED_STATE);
                XAStateLog.saveXARecoverylog(session.getSessionXaID(), mysqlCon);
                mysqlCon.setXaStatus(TxState.TX_INITIALIZE_STATE);
                if (decrementCountBy(1)) {
                    session.setXaState(TxState.TX_INITIALIZE_STATE);
                    cleanAndFeedback();
                }

                // 'xa rollback' ok
            } else if (mysqlCon.getXaStatus() == TxState.TX_PREPARED_STATE || mysqlCon.getXaStatus() == TxState.TX_PREPARE_UNCONNECT_STATE || mysqlCon.getXaStatus() == TxState.TX_ROLLBACK_FAILED_STATE) { // we dont' konw if the conn prepared or not


                mysqlCon.setXaStatus(TxState.TX_ROLLBACKED_STATE);
                XAStateLog.saveXARecoverylog(session.getSessionXaID(), mysqlCon);
                mysqlCon.setXaStatus(TxState.TX_INITIALIZE_STATE);
                if (decrementCountBy(1)) {
                    if (session.getXaState() == TxState.TX_PREPARED_STATE) {
                        session.setXaState(TxState.TX_INITIALIZE_STATE);
                    }
                    cleanAndFeedback();
                }

            } else { // LOGGER.error("Wrong XA status flag!");
            }
        }
    }

    @Override
    public void errorResponse(byte[] err, BackendConnection conn) {
        if (conn instanceof MySQLConnection) {
            MySQLConnection mysqlCon = (MySQLConnection) conn;
            if (mysqlCon.getXaStatus() == TxState.TX_STARTED_STATE) {
                mysqlCon.quit();
                mysqlCon.setXaStatus(TxState.TX_ROLLBACKED_STATE);
                XAStateLog.saveXARecoverylog(session.getSessionXaID(), mysqlCon);
                if (decrementCountBy(1)) {
                    session.setXaState(TxState.TX_ENDED_STATE);
                    rollback();
                }

                // 'xa rollback' ok without prepared
            } else if (mysqlCon.getXaStatus() == TxState.TX_ENDED_STATE) {
                mysqlCon.quit();
                mysqlCon.setXaStatus(TxState.TX_ROLLBACKED_STATE);
                XAStateLog.saveXARecoverylog(session.getSessionXaID(), mysqlCon);
                if (decrementCountBy(1)) {
                    session.setXaState(TxState.TX_INITIALIZE_STATE);
                    cleanAndFeedback();
                }

                // 'xa rollback' err
            } else if (mysqlCon.getXaStatus() == TxState.TX_ROLLBACK_FAILED_STATE || mysqlCon.getXaStatus() == TxState.TX_PREPARED_STATE) {
                mysqlCon.setXaStatus(TxState.TX_ROLLBACK_FAILED_STATE);
                mysqlCon.quit();
                XAStateLog.saveXARecoverylog(session.getSessionXaID(), mysqlCon);
                session.setXaState(TxState.TX_ROLLBACK_FAILED_STATE);
                if (decrementCountBy(1)) {
                    cleanAndFeedback();
                }

                // we dont' konw if the conn prepared or not
            } else if (mysqlCon.getXaStatus() == TxState.TX_PREPARE_UNCONNECT_STATE) {
                ErrorPacket errPacket = new ErrorPacket();
                errPacket.read(err);
                if (errPacket.errno == ErrorCode.ER_XAER_NOTA) {
                    //ERROR 1397 (XAE04): XAER_NOTA: Unknown XID, not prepared
                    mysqlCon.setXaStatus(TxState.TX_ROLLBACKED_STATE);
                    XAStateLog.saveXARecoverylog(session.getSessionXaID(), mysqlCon);
                    mysqlCon.setXaStatus(TxState.TX_INITIALIZE_STATE);
                    if (decrementCountBy(1)) {
                        if (session.getXaState() == TxState.TX_PREPARED_STATE) {
                            session.setXaState(TxState.TX_INITIALIZE_STATE);
                        }
                        cleanAndFeedback();
                    }
                } else {
                    session.setXaState(TxState.TX_ROLLBACK_FAILED_STATE);
                    XAStateLog.saveXARecoverylog(session.getSessionXaID(), mysqlCon);
                    if (decrementCountBy(1)) {
                        cleanAndFeedback();
                    }
                }

            } else { // LOGGER.error("Wrong XA status flag!");
            }
        }
    }

    @Override
    public void connectionError(Throwable e, BackendConnection conn) {
        if (conn instanceof MySQLConnection) {
            MySQLConnection mysqlCon = (MySQLConnection) conn;
            if (mysqlCon.getXaStatus() == TxState.TX_STARTED_STATE) {
                mysqlCon.quit();
                mysqlCon.setXaStatus(TxState.TX_ROLLBACKED_STATE);
                XAStateLog.saveXARecoverylog(session.getSessionXaID(), mysqlCon);
                if (decrementCountBy(1)) {
                    session.setXaState(TxState.TX_ENDED_STATE);
                    rollback();
                }

                // 'xa rollback' ok without prepared
            } else if (mysqlCon.getXaStatus() == TxState.TX_ENDED_STATE) {
                mysqlCon.quit();
                mysqlCon.setXaStatus(TxState.TX_ROLLBACKED_STATE);
                XAStateLog.saveXARecoverylog(session.getSessionXaID(), mysqlCon);
                if (decrementCountBy(1)) {
                    session.setXaState(TxState.TX_INITIALIZE_STATE);
                    cleanAndFeedback();
                }

                // 'xa rollback' err
            } else if (mysqlCon.getXaStatus() == TxState.TX_PREPARED_STATE) {
                mysqlCon.setXaStatus(TxState.TX_ROLLBACK_FAILED_STATE);
                mysqlCon.quit();
                XAStateLog.saveXARecoverylog(session.getSessionXaID(), mysqlCon);
                session.setXaState(TxState.TX_ROLLBACK_FAILED_STATE);
                if (decrementCountBy(1)) {
                    cleanAndFeedback();
                }

                // we dont' konw if the conn prepared or not
            } else if (mysqlCon.getXaStatus() == TxState.TX_PREPARE_UNCONNECT_STATE) {
                session.setXaState(TxState.TX_ROLLBACK_FAILED_STATE);
                XAStateLog.saveXARecoverylog(session.getSessionXaID(), mysqlCon);
                if (decrementCountBy(1)) {
                    cleanAndFeedback();
                }

            } else { // LOGGER.error("Wrong XA status flag!");
            }
        }
    }

    @Override
    public void connectionClose(BackendConnection conn, String reason) {
        this.setFail(reason);
        if (conn instanceof MySQLConnection) {
            MySQLConnection mysqlCon = (MySQLConnection) conn;
            if (mysqlCon.getXaStatus() == TxState.TX_STARTED_STATE) {
                mysqlCon.quit();
                mysqlCon.setXaStatus(TxState.TX_ROLLBACKED_STATE);
                XAStateLog.saveXARecoverylog(session.getSessionXaID(), mysqlCon);
                if (decrementCountBy(1)) {
                    session.setXaState(TxState.TX_ENDED_STATE);
                    rollback();
                }

                // 'xa rollback' ok without prepared
            } else if (mysqlCon.getXaStatus() == TxState.TX_ENDED_STATE) {
                mysqlCon.quit();
                mysqlCon.setXaStatus(TxState.TX_ROLLBACKED_STATE);
                XAStateLog.saveXARecoverylog(session.getSessionXaID(), mysqlCon);
                if (decrementCountBy(1)) {
                    session.setXaState(TxState.TX_INITIALIZE_STATE);
                    cleanAndFeedback();
                }

                // 'xa rollback' err
            } else if (mysqlCon.getXaStatus() == TxState.TX_PREPARED_STATE) {
                mysqlCon.setXaStatus(TxState.TX_ROLLBACK_FAILED_STATE);
                mysqlCon.quit();
                XAStateLog.saveXARecoverylog(session.getSessionXaID(), mysqlCon);
                session.setXaState(TxState.TX_ROLLBACK_FAILED_STATE);
                if (decrementCountBy(1)) {
                    cleanAndFeedback();
                }

            } else { // LOGGER.error("Wrong XA status flag!");
            }
        }
    }

    private void cleanAndFeedback() {
        if (session.getXaState() == TxState.TX_INITIALIZE_STATE) { // clear all resources
            XAStateLog.saveXARecoverylog(session.getSessionXaID(), TxState.TX_ROLLBACKED_STATE);
            //取消限制
            session.cancelableStatusSet(NonBlockingSession.CANCEL_STATUS_INIT);
            byte[] send = sendData;
            session.clearResources(false);
            if (session.closed()) {
                return;
            }
            session.getSource().write(send);

            //partitionly commited,must commit again
        } else if (session.getXaState() == TxState.TX_ROLLBACK_FAILED_STATE || session.getXaState() == TxState.TX_PREPARED_STATE) {
            MySQLConnection errConn = session.releaseExcept(session.getXaState());
            if (errConn != null) {
                XAStateLog.saveXARecoverylog(session.getSessionXaID(), session.getXaState());
                if (++tryRollbackTimes < ROLLBACK_TIMES) {
                    // 多试几次
                    rollback();
                } else {
                    StringBuilder closeReason = new StringBuilder("ROLLBCAK FAILED but it will try to ROLLBACK repeatedly in backend until it is success!");
                    if (error != null) {
                        closeReason.append(", the ERROR is ");
                        closeReason.append(error);
                    }
                    // 关session ,add to定时任务
                    session.getSource().close(closeReason.toString());
                    MycatServer.getInstance().getXaSessionCheck().addRollbackSession(session);
                }
            } else {
                XAStateLog.saveXARecoverylog(session.getSessionXaID(), TxState.TX_ROLLBACKED_STATE);
                session.setXaState(TxState.TX_INITIALIZE_STATE);
                byte[] toSend = sendData;
                session.clearResources(false);
                if (!session.closed()) {
                    session.getSource().write(toSend);
                }
            }

            // rollbak success,but closed coon must remove
        } else {
            removeQuitConn();
            XAStateLog.saveXARecoverylog(session.getSessionXaID(), TxState.TX_ROLLBACKED_STATE);
            session.setXaState(TxState.TX_INITIALIZE_STATE);
            session.clearResources(false);
            if (session.closed()) {
                return;
            }
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


    public void debugRollbackDelay() {
        try {
            if (LOGGER.isDebugEnabled() && System.getProperty(SystemConfig.XA_ROLLBACK_DELAY) != null) {
                long delayTime = System.getProperty(SystemConfig.XA_ROLLBACK_DELAY) == null ?
                        0 : Long.parseLong(System.getProperty(SystemConfig.XA_ROLLBACK_DELAY)) * 1000;
                LOGGER.debug("before xa rollback sleep time = " + delayTime);
                Thread.sleep(delayTime);
                LOGGER.debug("before xa rollback sleep finished " + delayTime);
            }
        } catch (Exception e) {
            LOGGER.debug("before xa rollback  sleep error ");
        }
    }

}
