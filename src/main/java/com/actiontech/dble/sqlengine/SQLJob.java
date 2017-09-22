/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sqlengine;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.datasource.PhysicalDBNode;
import com.actiontech.dble.backend.datasource.PhysicalDatasource;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.parser.ServerParse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * asyn execute in EngineCtx or standalone (EngineCtx=null)
 *
 * @author wuzhih
 */
public class SQLJob implements ResponseHandler, Runnable {

    public static final Logger LOGGER = LoggerFactory.getLogger(SQLJob.class);

    private final String sql;
    private final String dataNodeOrDatabase;
    private BackendConnection connection;
    private final SQLJobHandler jobHandler;
    private final PhysicalDatasource ds;
    private volatile boolean finished;


    public SQLJob(String sql, String databaseName, SQLJobHandler jobHandler,
                  PhysicalDatasource ds) {
        super();
        this.sql = sql;
        this.dataNodeOrDatabase = databaseName;
        this.jobHandler = jobHandler;
        this.ds = ds;

    }

    public void run() {
        try {
            if (ds == null) {
                RouteResultsetNode node = new RouteResultsetNode(
                        dataNodeOrDatabase, ServerParse.SELECT, sql);
                // create new connection
                ServerConfig conf = DbleServer.getInstance().getConfig();
                PhysicalDBNode dn = conf.getDataNodes().get(node.getName());
                dn.getConnection(dn.getDatabase(), true, node, this, node);
            } else {
                ds.getConnection(dataNodeOrDatabase, true, this, null);
            }
        } catch (Exception e) {
            LOGGER.info("can't get connection for sql ,error:" + e);
            doFinished(true);
        }
    }

    public void teminate(String reason) {
        LOGGER.info("terminate this job reason:" + reason + " con:" + connection + " sql " + this.sql);
        if (connection != null) {
            connection.close(reason);
        }
    }

    @Override
    public void connectionAcquired(final BackendConnection conn) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("con query sql:" + sql + " to con:" + conn);
        }
        conn.setResponseHandler(this);
        try {
            conn.query(sql);
            connection = conn;
        } catch (Exception e) { // (UnsupportedEncodingException e) {
            doFinished(true);
        }

    }

    public boolean isFinished() {
        return finished;
    }

    private void doFinished(boolean failed) {
        finished = true;
        jobHandler.finished(dataNodeOrDatabase, failed);
    }

    @Override
    public void connectionError(Throwable e, BackendConnection conn) {
        LOGGER.info("can't get connection for sql :" + sql);
        doFinished(true);
    }

    @Override
    public void errorResponse(byte[] err, BackendConnection conn) {
        ErrorPacket errPg = new ErrorPacket();
        errPg.read(err);

        String errMsg = "error response errno:" + errPg.getErrno() + ", " + new String(errPg.getMessage()) +
                " from of sql :" + sql + " at con:" + conn;


        if (errPg.getErrno() == ErrorCode.ER_SPECIFIC_ACCESS_DENIED_ERROR) {
            // @see https://dev.mysql.com/doc/refman/5.6/en/error-messages-server.html
            LOGGER.warn(errMsg);
        } else if (errPg.getErrno() == ErrorCode.ER_XAER_NOTA) {
            // ERROR 1397 (XAE04): XAER_NOTA: Unknown XID, not prepared
            conn.release();
            doFinished(false);
            return;
        } else {
            LOGGER.info(errMsg);
        }
        conn.release();
        doFinished(true);
    }

    @Override
    public void okResponse(byte[] ok, BackendConnection conn) {
        if (conn.syncAndExecute()) {
            conn.release();
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
        boolean finsihed = jobHandler.onRowData(dataNodeOrDatabase, row);
        if (finsihed) {
            conn.release();
            doFinished(false);
        }
        return false;
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, BackendConnection conn) {
        conn.release();
        doFinished(false);
    }

    @Override
    public void writeQueueAvailable() {

    }

    @Override
    public void connectionClose(BackendConnection conn, String reason) {
        doFinished(true);
    }
    @Override
    public String toString() {
        return "SQLJob [dataNodeOrDatabase=" +
                dataNodeOrDatabase + ",sql=" + sql + ",  jobHandler=" +
                jobHandler + "]";
    }

    @Override
    public void relayPacketResponse(byte[] relayPacket, BackendConnection conn) {
    }

    @Override
    public void endPacketResponse(byte[] endPacket, BackendConnection conn) {
    }

}
