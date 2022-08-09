/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.heartbeat;

import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.sqlengine.SQLJobHandler;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class HeartbeatSQLJob implements ResponseHandler {

    public static final Logger LOGGER = LoggerFactory.getLogger(HeartbeatSQLJob.class);

    private final String sql;
    private final SQLJobHandler jobHandler;
    private BackendConnection connection;
    private AtomicBoolean finished = new AtomicBoolean(false);
    private MySQLHeartbeat heartbeat;
    private long responseTime;
    private long keepAlive;

    public HeartbeatSQLJob(MySQLHeartbeat heartbeat, SQLJobHandler jobHandler) {
        super();
        this.sql = "/*# from=" + SystemConfig.getInstance().getInstanceName() + " reason=heartbeat*/" + heartbeat.getHeartbeatSQL();
        this.jobHandler = jobHandler;
        this.heartbeat = heartbeat;
        this.responseTime = System.nanoTime();
        this.keepAlive = TimeUnit.NANOSECONDS.convert(heartbeat.getKeepAlive(), TimeUnit.SECONDS) + TimeUnit.NANOSECONDS.convert(heartbeat.getSource().getConfig().getPoolConfig().getHeartbeatPeriodMillis(), TimeUnit.MILLISECONDS);
    }

    public void terminate() {
        if (connection != null && !connection.isClosed()) {
            String errMsg = heartbeat.getMessage() == null ? "heart beat quit" : heartbeat.getMessage();
            LOGGER.info("[heartbeat]terminate this job reason:" + errMsg + " con:" + connection + " sql " + this.sql);
            connection.businessClose("[heartbeat] quit");
        }
    }

    @Override
    public void connectionAcquired(final BackendConnection conn) {
        this.connection = conn;
        conn.getBackendService().setResponseHandler(this);
        conn.getBackendService().setComplexQuery(true);
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("[heartbeat]do heartbeat,conn is " + conn);
            }
            updateResponseTime();
            conn.getBackendService().query(sql);
        } catch (Exception e) { // (UnsupportedEncodingException e) {
            LOGGER.warn("[heartbeat]send heartbeat error", e);
            heartbeat.setErrorResult("send heartbeat error, because of [" + e.getMessage() + "]");
            doFinished(true);
        }
    }

    public void execute() {
        // reset
        finished.set(false);
        if (connection == null) {
            LOGGER.warn("[heartbeat]connect timeout,please pay attention to network latency or packet loss.");
            heartbeat.setErrorResult("connect timeout");
            doFinished(true);
        } else {
            try {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("[heartbeat]do heartbeat,conn is " + connection);
                }
                if (System.nanoTime() > responseTime + keepAlive) {
                    String reason = "[heartbeat]connect timeout，the connection may be unreachable for a long time due to TCP retransmission";
                    LOGGER.warn(reason);
                    heartbeat.setErrorResult(reason);
                    doFinished(true);
                    return;
                }
                connection.getBackendService().query(sql);
            } catch (Exception e) { // (UnsupportedEncodingException e) {
                LOGGER.warn("[heartbeat]send heartbeat error", e);
                heartbeat.setErrorResult("send heartbeat error, because of [" + e.getMessage() + "]");
                doFinished(true);
            }
        }
    }

    private void doFinished(boolean failed) {
        if (finished.compareAndSet(false, true)) {
            jobHandler.finished(null, failed);
        }
    }

    @Override
    public void connectionError(Throwable e, Object attachment) {
        LOGGER.warn("[heartbeat]can't get connection for sql :" + sql, e);
        updateResponseTime();
        heartbeat.setErrorResult("heartbeat connection Error");
        doFinished(true);
    }

    @Override
    public void errorResponse(byte[] err, @NotNull AbstractService service) {
        ErrorPacket errPg = new ErrorPacket();
        errPg.read(err);
        MySQLResponseService responseService = (MySQLResponseService) service;
        LOGGER.warn("[heartbeat]error response errNo: {}, {} from of sql: {} at con: {} db user = {}",
                errPg.getErrNo(), new String(errPg.getMessage()), sql, service,
                responseService.getConnection().getInstance().getConfig().getUser());
        updateResponseTime();
        heartbeat.setErrorResult(new String(errPg.getMessage()));
        if (!((MySQLResponseService) service).syncAndExecute()) {
            service.getConnection().businessClose("[heartbeat]unfinished sync");
            doFinished(true);
            return;
        }
        doFinished(true);
    }

    @Override
    public void okResponse(byte[] ok, @NotNull AbstractService service) {
        updateResponseTime();
        if (((MySQLResponseService) service).syncAndExecute()) {
            doFinished(false);
        }
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof,
                                 boolean isLeft, @NotNull AbstractService service) {
        updateResponseTime();
        jobHandler.onHeader(fields);

    }

    @Override
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, @NotNull AbstractService service) {
        updateResponseTime();
        jobHandler.onRowData(row);
        return false;
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, @NotNull AbstractService service) {
        updateResponseTime();
        doFinished(false);
    }

    @Override
    public void connectionClose(@NotNull AbstractService service, String reason) {
        LOGGER.warn("[heartbeat]conn for sql[" + sql + "] is closed, due to " + reason + ", we will try again immediately");
        updateResponseTime();
        if (!heartbeat.doHeartbeatRetry()) {
            heartbeat.setErrorResult("heartbeat conn for sql[" + sql + "] is closed, due to " + reason);
            doFinished(true);
        }
    }

    private void updateResponseTime() {
        responseTime = System.nanoTime();
    }

    @Override
    public String toString() {
        return "HeartbeatSQLJob [sql=" + sql + ",  jobHandler=" + jobHandler + ", backend conn" + connection + "]";
    }

}
