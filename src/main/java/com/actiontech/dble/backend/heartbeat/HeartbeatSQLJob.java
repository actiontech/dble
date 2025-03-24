/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.heartbeat;

import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.sqlengine.SQLJobHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicStampedReference;

public class HeartbeatSQLJob implements ResponseHandler {

    public static final Logger LOGGER = LoggerFactory.getLogger(HeartbeatSQLJob.class);

    private volatile String sql;
    private final SQLJobHandler jobHandler;
    /*
     *   (null, 0) -> initial
     *   (conn, 1) -> heartbeat
     *   (null, 2) -> quit
     */
    private final AtomicStampedReference<BackendConnection> connectionRef = new AtomicStampedReference<>(null, 0);
    private final AtomicBoolean finished = new AtomicBoolean(false);
    private final MySQLHeartbeat heartbeat;

    public HeartbeatSQLJob(MySQLHeartbeat heartbeat, SQLJobHandler jobHandler) {
        super();
        this.jobHandler = jobHandler;
        this.heartbeat = heartbeat;
    }

    public long getConnectionId() {
        final BackendConnection con = this.connectionRef.getReference();
        long connId = 0;
        if (con != null) {
            connId = con.getId();
        }
        return connId;
    }

    public void terminate() {
        if (connectionRef.compareAndSet(null, null, 0, 2)) {
            LOGGER.info("[heartbeat]terminate timeout heartbeat job.");
            return;
        }

        final BackendConnection con = this.connectionRef.getReference();
        connectionRef.set(null, 2);
        if (con != null && !con.isClosed()) {
            String errMsg = heartbeat.getMessage() == null ? "heart beat quit" : heartbeat.getMessage();
            LOGGER.info("[heartbeat]terminate this job reason:" + errMsg + " con:" + con + " sql " + this.sql);
            con.businessClose("[heartbeat] quit");
        }
    }

    @Override
    public void connectionAcquired(final BackendConnection conn) {
        if (!connectionRef.compareAndSet(null, conn, 0, 1)) {
            String errMsg = "[heartbeat]timeout connection[id=" + conn.getId() + "] is acquired, but the conn is useless.";
            LOGGER.info(errMsg);
            conn.businessClose(errMsg);
            return;
        }

        conn.getBackendService().setResponseHandler(this);
        conn.getBackendService().setComplexQuery(true);
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("[heartbeat]do heartbeat,conn is " + conn);
            }
            this.sql = heartbeat.getHeartbeatSQL();
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
        final BackendConnection conn = connectionRef.getReference();
        if (conn != null) {
            try {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("[heartbeat]do heartbeat,conn is {}", conn);
                }
                this.sql = heartbeat.getHeartbeatSQL();
                conn.getBackendService().query(sql);
            } catch (Exception e) { // (UnsupportedEncodingException e) {
                LOGGER.warn("[heartbeat]send heartbeat error", e);
                heartbeat.setErrorResult("send heartbeat error, because of [" + e.getMessage() + "]");
                doFinished(true);
            }
            return;
        }

        // heartbeat connection had been closed
        if (connectionRef.getStamp() == 2) {
            LOGGER.info("[heartbeat]connection had been closed.");
        } else {
            LOGGER.warn("[heartbeat]connect timeout,please pay attention to network latency or packet loss.");
            heartbeat.setErrorResult("connect timeout");
            doFinished(true);
        }
    }

    private void doFinished(boolean failed) {
        if (finished.compareAndSet(false, true)) {
            jobHandler.finished(null, failed);
        }
    }

    @Override
    public void connectionError(Throwable e, Object attachment) {
        LOGGER.warn("[heartbeat]can't get connection for sql : {}", sql, e);
        heartbeat.setErrorResult("heartbeat connection Error");
        doFinished(true);
    }

    @Override
    public void errorResponse(byte[] err, AbstractService service) {
        ErrorPacket errPg = new ErrorPacket();
        errPg.read(err);
        MySQLResponseService responseService = (MySQLResponseService) service;
        LOGGER.warn("[heartbeat]error response errNo: {}, {} from of sql: {} at con: {} db user = {}",
                errPg.getErrNo(), new String(errPg.getMessage()), sql, service,
                responseService.getConnection().getInstance().getConfig().getUser());
        heartbeat.setErrorResult(new String(errPg.getMessage()));
        if (!((MySQLResponseService) service).syncAndExecute()) {
            service.getConnection().businessClose("[heartbeat]unfinished sync");
            doFinished(true);
            return;
        }
        doFinished(true);
    }

    @Override
    public void okResponse(byte[] ok, AbstractService service) {
        if (((MySQLResponseService) service).syncAndExecute()) {
            doFinished(false);
        }
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof,
                                 boolean isLeft, AbstractService service) {
        jobHandler.onHeader(fields);

    }

    @Override
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, AbstractService service) {
        jobHandler.onRowData(row);
        return false;
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, AbstractService service) {
        doFinished(false);
    }

    @Override
    public void connectionClose(AbstractService service, String reason) {
        LOGGER.warn("[heartbeat]conn for sql[{}] is closed, due to {}, we will try again immediately", sql, reason);
        if (!heartbeat.doHeartbeatRetry()) {
            heartbeat.setErrorResult("heartbeat conn for sql[" + sql + "] is closed, due to " + reason);
            doFinished(true);
        }
    }

    @Override
    public String toString() {
        return "HeartbeatSQLJob [sql=" + sql + ", isQuit=" + isQuit() + ",  jobHandler=" + jobHandler + ", backend conn" + connectionRef.getReference() + "]";
    }

    public boolean isQuit() {
        return connectionRef.getStamp() == 2;
    }
}
