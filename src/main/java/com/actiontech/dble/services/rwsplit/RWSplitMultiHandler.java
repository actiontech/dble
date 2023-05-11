/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.rwsplit;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.log.sqldump.SqlDumpLogHelper;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.net.mysql.StatusFlags;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.net.service.ResultFlag;
import com.actiontech.dble.net.service.WriteFlags;
import com.actiontech.dble.rwsplit.RWSplitNonBlockingSession;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class RWSplitMultiHandler extends RWSplitHandler {

    private final AtomicBoolean pauseFlag = new AtomicBoolean(false);
    private final Lock lock;
    private final Condition done;

    public RWSplitMultiHandler(RWSplitService service, boolean isUseOriginPacket, byte[] originPacket, Callback callback) {
        super(service, isUseOriginPacket, originPacket, callback);
        this.lock = new ReentrantLock();
        this.done = lock.newCondition();
    }

    @Override
    public void execute(BackendConnection conn) {
        rwSplitService.setMultiHandler(this);
        super.execute(conn);
    }

    public void executeNext(Callback cb) {
        this.executeSql = rwSplitService.getExecuteSql();
        this.callback = cb;
        extractNextSqlResult();
    }

    @Override
    public void okResponse(byte[] data, @NotNull AbstractService service) {
        MySQLResponseService mysqlService = (MySQLResponseService) service;
        this.netOutBytes += data.length;
        boolean executeResponse = mysqlService.syncAndExecute();
        if (executeResponse) {
            this.resultSize += data.length;
            final OkPacket packet = new OkPacket();
            packet.read(data);
            loadDataClean();
            synchronized (this) {
                if (!write2Client) {
                    rwSplitService.getSession2().recordLastSqlResponseTime();
                    SqlDumpLogHelper.info(executeSql, originPacket, rwSplitService, mysqlService, packet.getAffectedRows());
                    if (callback != null)
                        callback.callback(true, null, rwSplitService);
                    rwSplitService.getSession2().trace(t -> t.setBackendResponseEndTime(mysqlService));
                    data[3] = (byte) rwSplitService.nextPacketId();
                    if ((packet.getServerStatus() & StatusFlags.SERVER_MORE_RESULTS_EXISTS) == 0) {
                        resetMultiStatus();
                        rwSplitService.getSession2().unbindIfSafe();
                        rwSplitService.getSession2().trace(t -> t.doSqlStat(packet.getAffectedRows(), data.length, data.length));
                        rwSplitService.write(data, WriteFlags.QUERY_END, ResultFlag.OK);
                        write2Client = true;
                    } else {
                        rwSplitService.getSession2().trace(t -> t.doSqlStat(packet.getAffectedRows(), data.length, data.length));
                        rwSplitService.write(data, WriteFlags.MULTI_QUERY_PART, ResultFlag.OK);
                        extractNextSql();
                    }

                }
            }
        }
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, @NotNull AbstractService service) {
        synchronized (this) {
            if (!write2Client) {
                this.netOutBytes += eof.length;
                this.resultSize += eof.length;
                rwSplitService.getSession2().recordLastSqlResponseTime();
                if (callback != null)
                    callback.callback(true, null, rwSplitService);
                SqlDumpLogHelper.info(executeSql, originPacket, rwSplitService, (MySQLResponseService) service, selectRows);
                eof[3] = (byte) rwSplitService.nextPacketId();
                if ((eof[7] & StatusFlags.SERVER_MORE_RESULTS_EXISTS) == 0) {
                    resetMultiStatus();
                    rwSplitService.getSession2().unbindIfSafe();
                    rwSplitService.getSession2().trace(t -> t.doSqlStat(selectRows, netOutBytes, resultSize));
                    buffer = frontedConnection.getService().writeToBuffer(eof, buffer);
                    frontedConnection.getService().writeDirectly(buffer, WriteFlags.QUERY_END, ResultFlag.EOF_ROW);
                    buffer = null;
                    selectRows = 0;
                    write2Client = true;
                } else {
                    /*
                    multi statement all cases are as follows:
                    1. if an resultSet is followed by an resultSet. buffer will re-assign in fieldEofResponse()
                    2. if an resultSet is followed by an okResponse. okResponse() send directly without use buffer.
                    3. if an resultSet is followed by  an errorResponse. buffer will be used if it is not null.

                    We must prevent  same buffer called connection.write() twice.
                    According to the above, you need write buffer immediately and set buffer to null.
                     */
                    if (LOGGER.isDebugEnabled())
                        LOGGER.debug("Because of multi query had send.It would receive more than one ResultSet. recycle resource should be delayed. client:{}", rwSplitService);
                    rwSplitService.getSession2().trace(t -> t.doSqlStat(selectRows, netOutBytes, resultSize));
                    buffer = frontedConnection.getService().writeToBuffer(eof, buffer);
                    frontedConnection.getService().writeDirectly(buffer, WriteFlags.MULTI_QUERY_PART, ResultFlag.EOF_ROW);
                    buffer = null;
                    selectRows = 0;
                    extractNextSql();
                }
            }
        }
    }

    @Override
    public void errorResponse(byte[] data, @NotNull AbstractService service) {
        resetMultiStatus();
        super.errorResponse(data, service);
    }

    @Override
    protected void writeErrorMsg(int pId, String reason) {
        resetMultiStatus();
        super.writeErrorMsg(pId, reason);
    }

    private void extractNextSql() {
        lock.lock();
        try {
            netOutBytes = 0;
            resultSize = 0;
            pauseFlag.set(true);
            DbleServer.getInstance().getComplexQueryExecutor().execute(() -> {
                rwSplitService.getSession().trace(t -> t.startProcess());
                rwSplitService.handleComQuery(rwSplitService.getExecuteSqlBytes());
            });
            while (pauseFlag.get()) {
                done.await();
            }
        } catch (InterruptedException e) {
            // ignore
            LOGGER.error("RWSplitMultiHandler.extractNextSql() exception ", e);
        } finally {
            lock.unlock();
        }
    }

    private void extractNextSqlResult() {
        lock.lock();
        try {
            pauseFlag.set(false);
            done.signal();
        } finally {
            lock.unlock();
        }
    }

    private void resetMultiStatus() {
        RWSplitNonBlockingSession session = rwSplitService.getSession2();
        if (session.getIsMultiStatement().compareAndSet(true, false)) {
            session.setRemainingSql(null);
            rwSplitService.setMultiHandler(null);
            pauseFlag.set(false);
        }
    }
}
