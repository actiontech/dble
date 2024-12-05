/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.rwsplit;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.log.sqldump.SqlDumpLogHelper;
import com.oceanbase.obsharding_d.net.connection.BackendConnection;
import com.oceanbase.obsharding_d.net.mysql.OkPacket;
import com.oceanbase.obsharding_d.net.mysql.StatusFlags;
import com.oceanbase.obsharding_d.net.service.AbstractService;
import com.oceanbase.obsharding_d.net.service.ResultFlag;
import com.oceanbase.obsharding_d.net.service.WriteFlags;
import com.oceanbase.obsharding_d.rwsplit.RWSplitNonBlockingSession;
import com.oceanbase.obsharding_d.services.mysqlsharding.MySQLResponseService;
import com.oceanbase.obsharding_d.statistic.sql.StatisticListener;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class RWSplitMultiHandler extends RWSplitHandler {

    private final AtomicBoolean pauseFlag = new AtomicBoolean(false);
    private final Lock lock;
    private final Condition done;
    private BackendConnection backendConnection;

    public RWSplitMultiHandler(RWSplitService service, boolean isUseOriginPacket, byte[] originPacket, Callback callback) {
        super(service, isUseOriginPacket, originPacket, callback);
        this.lock = new ReentrantLock();
        this.done = lock.newCondition();
    }

    @Override
    public void execute(BackendConnection conn) {
        rwSplitService.setMultiHandler(this);
        this.backendConnection = conn;
        super.execute(conn);
    }

    public void executeNext(Callback cb) {
        this.executeSql = rwSplitService.getExecuteSql();
        this.callback = cb;
        StatisticListener.getInstance().record(rwSplitService, r -> r.onBackendSqlStart(backendConnection));
        extractNextSqlResult();
    }

    @Override
    public void okResponse(byte[] data, @NotNull AbstractService service) {
        MySQLResponseService mysqlService = (MySQLResponseService) service;
        boolean executeResponse = mysqlService.syncAndExecute();
        if (executeResponse) {
            final OkPacket packet = new OkPacket();
            packet.read(data);
            loadDataClean();
            synchronized (this) {
                if (!write2Client) {
                    rwSplitService.getSession2().recordLastSqlResponseTime();
                    StatisticListener.getInstance().record(rwSplitService, r -> r.onBackendSqlSetRowsAndEnd(packet.getAffectedRows()));
                    SqlDumpLogHelper.info(executeSql, originPacket, rwSplitService, mysqlService, packet.getAffectedRows());
                    if (callback != null)
                        callback.callback(true, null, rwSplitService);
                    data[3] = (byte) rwSplitService.nextPacketId();
                    if ((packet.getServerStatus() & StatusFlags.SERVER_MORE_RESULTS_EXISTS) == 0) {
                        resetMultiStatus();
                        rwSplitService.getSession2().unbindIfSafe();
                        rwSplitService.write(data, WriteFlags.QUERY_END, ResultFlag.OK);
                        write2Client = true;
                    } else {
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
                rwSplitService.getSession2().recordLastSqlResponseTime();
                if (callback != null)
                    callback.callback(true, null, rwSplitService);
                StatisticListener.getInstance().record(rwSplitService, r -> r.onBackendSqlSetRowsAndEnd(selectRows));
                SqlDumpLogHelper.info(executeSql, originPacket, rwSplitService, (MySQLResponseService) service, selectRows);
                eof[3] = (byte) rwSplitService.nextPacketId();
                if ((eof[7] & StatusFlags.SERVER_MORE_RESULTS_EXISTS) == 0) {
                    resetMultiStatus();
                    rwSplitService.getSession2().unbindIfSafe();
                    buffer = frontedConnection.getService().writeToBuffer(eof, buffer);
                    frontedConnection.getService().writeDirectly(buffer, WriteFlags.QUERY_END);
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
                    buffer = frontedConnection.getService().writeToBuffer(eof, buffer);
                    frontedConnection.getService().writeDirectly(buffer, WriteFlags.MULTI_QUERY_PART);
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
            pauseFlag.set(true);
            OBsharding_DServer.getInstance().getComplexQueryExecutor().execute(() -> {
                StatisticListener.getInstance().record(rwSplitService, r -> r.onFrontendSqlStart());
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
