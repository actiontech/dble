/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.heartbeat;

import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
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
        String errMsg = heartbeat.getMessage() == null ? "heart beat quit" : heartbeat.getMessage();
        LOGGER.info("terminate this job reason:" + errMsg + " con:" + connection + " sql " + this.sql);
        if (connection != null) {
            connection.closeWithoutRsp("heartbeat quit");
        }
    }

    @Override
    public void connectionAcquired(final BackendConnection conn) {
        this.connection = conn;
        conn.setResponseHandler(this);
        ((MySQLConnection) conn).setComplexQuery(true);
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("do heartbeat,conn is " + conn);
            }
            conn.query(sql);
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
            connection.query(sql);
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
    public void errorResponse(byte[] err, BackendConnection conn) {
        ErrorPacket errPg = new ErrorPacket();
        errPg.read(err);
        heartbeat.setErrorResult(new String(errPg.getMessage()));
        String errMsg = "error response errNo:" + errPg.getErrNo() + ", " + new String(errPg.getMessage()) +
                " from of sql :" + sql + " at con:" + conn;

        LOGGER.info(errMsg);
        if (!conn.syncAndExecute()) {
            conn.closeWithoutRsp("unfinished sync");
            doFinished(true);
            return;
        }
        doFinished(true);
    }

    @Override
    public void okResponse(byte[] ok, BackendConnection conn) {
        if (conn.syncAndExecute()) {
            doFinished(false);
        }
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof,
                                 boolean isLeft, BackendConnection conn) {
        jobHandler.onHeader(fields);

    }

    @Override
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        jobHandler.onRowData(row);
        return false;
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, BackendConnection conn) {
        doFinished(false);
    }

    @Override
    public void connectionClose(BackendConnection conn, String reason) {
        LOGGER.warn("heartbeat conn for sql[" + sql + "] is closed, due to " + reason + ", we will try immedia");
        if (heartbeat.isChecking()) {
            doFinished(false);
        }
        heartbeat.getSource().createConnectionSkipPool(null, this);
    }

    @Override
    public String toString() {
        return "HeartbeatSQLJob [sql=" + sql + ",  jobHandler=" +
                jobHandler + "]";
    }

}
