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

public class HeartbeatSQLJob implements ResponseHandler {

    public static final Logger LOGGER = LoggerFactory.getLogger(HeartbeatSQLJob.class);

    private final String sql;
    private final SQLJobHandler jobHandler;
    private BackendConnection connection;
    private AtomicBoolean finished = new AtomicBoolean(false);
    private MySQLHeartbeat heartbeat;

    public HeartbeatSQLJob(MySQLHeartbeat heartbeat, SQLJobHandler jobHandler) {
        super();
        this.sql = heartbeat.getHeartbeatSQL();
        this.jobHandler = jobHandler;
        this.heartbeat = heartbeat;
    }

    public void terminate() {
        if (connection != null && !connection.isClosed()) {
            String errMsg = heartbeat.getMessage() == null ? "heart beat quit" : heartbeat.getMessage();
            LOGGER.info("terminate this job reason:" + errMsg + " con:" + connection + " sql " + this.sql);
            connection.businessClose("heartbeat quit");
        }
    }

    @Override
    public void connectionAcquired(final BackendConnection conn) {
        this.connection = conn;
        conn.getBackendService().setResponseHandler(this);
        conn.getBackendService().setComplexQuery(true);
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("do heartbeat,conn is " + conn);
            }
            conn.getBackendService().query(sql);
        } catch (Exception e) { // (UnsupportedEncodingException e) {
            doFinished(true);
        }
    }

    public void execute() {
        // reset
        finished.set(false);
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("do heartbeat,conn is " + connection);
            }
            connection.getBackendService().query(sql);
        } catch (Exception e) { // (UnsupportedEncodingException e) {
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
        LOGGER.warn("can't get connection for sql :" + sql, e);
        heartbeat.setErrorResult("connection Error");
        doFinished(true);
    }

    @Override
    public void errorResponse(byte[] err, AbstractService service) {
        ErrorPacket errPg = new ErrorPacket();
        errPg.read(err);
        heartbeat.setErrorResult(new String(errPg.getMessage()));
        String errMsg = "error response errNo:" + errPg.getErrNo() + ", " + new String(errPg.getMessage()) +
                " from of sql :" + sql + " at con:" + service;

        LOGGER.info(errMsg);
        if (!((MySQLResponseService) service).syncAndExecute()) {
            service.getConnection().businessClose("unfinished sync");
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
        LOGGER.warn("heartbeat conn for sql[" + sql + "] is closed, due to " + reason + ", we will try again immediately");
        if (!heartbeat.doHeartbeatRetry()) {
            heartbeat.setErrorResult("heartbeat conn for sql[" + sql + "] is closed, due to " + reason);
            doFinished(true);
        }
    }

    @Override
    public String toString() {
        return "HeartbeatSQLJob [sql=" + sql + ",  jobHandler=" +
                jobHandler + "]";
    }

}
