/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sqlengine;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.datasource.PhysicalDBNode;
import com.actiontech.dble.backend.datasource.PhysicalDatasource;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.parser.ServerParse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * async execute in EngineCtx or standalone (EngineCtx=null)
 *
 * @author wuzhih
 */
public class SQLJob implements ResponseHandler, Runnable {

    public static final Logger LOGGER = LoggerFactory.getLogger(SQLJob.class);

    private final String sql;
    private final String dataNode;
    private final String schema;
    private BackendConnection connection;
    private final SQLJobHandler jobHandler;
    private final PhysicalDatasource ds;
    private boolean isMustWriteNode;
    private volatile boolean finished;

    public SQLJob(String sql, String schema, SQLJobHandler jobHandler, PhysicalDatasource ds) {
        super();
        this.sql = sql;
        this.jobHandler = jobHandler;
        this.ds = ds;
        this.schema = schema;
        this.dataNode = null;
    }

    public SQLJob(String sql, String dataNode, SQLJobHandler jobHandler, boolean isMustWriteNode) {
        super();
        this.sql = sql;
        this.jobHandler = jobHandler;
        this.ds = null;
        this.dataNode = dataNode;
        this.schema = null;
        this.isMustWriteNode = isMustWriteNode;
    }

    public void run() {
        try {
            if (ds == null) {
                RouteResultsetNode node = new RouteResultsetNode(dataNode, ServerParse.SELECT, sql);
                // create new connection
                PhysicalDBNode dn = DbleServer.getInstance().getConfig().getDataNodes().get(node.getName());
                dn.getConnection(dn.getDatabase(), isMustWriteNode, true, node, this, node);
            } else {
                ds.getConnection(schema, true, this, null);
            }
        } catch (Exception e) {
            LOGGER.warn("can't get connection", e);
            doFinished(true);
        }
    }

    public void terminate(String reason) {
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
        ((MySQLConnection) conn).setComplexQuery(true);
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
        jobHandler.finished(dataNode == null ? schema : dataNode, failed);
    }

    @Override
    public void connectionError(Throwable e, BackendConnection conn) {
        LOGGER.warn("can't get connection for sql :" + sql, e);
        doFinished(true);
    }

    @Override
    public void errorResponse(byte[] err, BackendConnection conn) {
        ErrorPacket errPg = new ErrorPacket();
        errPg.read(err);

        String errMsg = "error response errNo:" + errPg.getErrNo() + ", " + new String(errPg.getMessage()) +
                " from of sql :" + sql + " at con:" + conn;


        if (errPg.getErrNo() == ErrorCode.ER_SPECIFIC_ACCESS_DENIED_ERROR) {
            // @see https://dev.mysql.com/doc/refman/5.6/en/error-messages-server.html
            LOGGER.info(errMsg);
        } else if (errPg.getErrNo() == ErrorCode.ER_XAER_NOTA) {
            // ERROR 1397 (XAE04): XAER_NOTA: Unknown XID, not prepared
            conn.release();
            doFinished(false);
            return;
        } else {
            LOGGER.info(errMsg);
        }
        if (conn.syncAndExecute()) {
            conn.release();
        } else {
            ((MySQLConnection) conn).quit();
        }
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
        boolean finish = jobHandler.onRowData(row);
        if (finish) {
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
        return "SQLJob [dataNode=" +
                dataNode + ",schema=" +
                schema + ",sql=" + sql + ",  jobHandler=" +
                jobHandler + "]";
    }

}
