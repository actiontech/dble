/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sqlengine;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.datasource.PhysicalDataNode;
import com.actiontech.dble.backend.datasource.PhysicalDataSource;
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
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * async execute in EngineCtx or standalone (EngineCtx=null)
 *
 * @author wuzhih
 */
public class SQLJob implements ResponseHandler, Runnable, Cloneable {

    public static final Logger LOGGER = LoggerFactory.getLogger(SQLJob.class);

    private final String sql;
    private final String dataNode;
    private final String schema;
    private BackendConnection connection;
    private final SQLJobHandler jobHandler;
    private final PhysicalDataSource ds;
    private boolean isMustWriteNode = false;
    private AtomicBoolean finished = new AtomicBoolean(false);
    private volatile boolean testXid;

    public SQLJob(String sql, String schema, SQLJobHandler jobHandler, PhysicalDataSource ds) {
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
                PhysicalDataNode dn = DbleServer.getInstance().getConfig().getDataNodes().get(node.getName());
                dn.getConnection(dn.getDatabase(), isMustWriteNode, true, node, this, node);
            } else {
                ds.getConnection(schema, true, this, null, isMustWriteNode);
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
            conn.query(sql, true);
            connection = conn;
        } catch (Exception e) { // (UnsupportedEncodingException e) {
            doFinished(true);
        }

    }

    public boolean isFinished() {
        return finished.get();
    }

    protected boolean doFinished(boolean failed) {
        if (finished.compareAndSet(false, true)) {
            jobHandler.finished(dataNode == null ? schema : dataNode, failed);
            return true;
        }
        return false;
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

        LOGGER.info(errMsg);
        if (!conn.syncAndExecute()) {
            conn.closeWithoutRsp("unfinished sync");
            doFinished(true);
            return;
        }

        if (errPg.getErrNo() == ErrorCode.ER_XAER_NOTA) {
            // ERROR 1397 (XAE04): XAER_NOTA: Unknown XID, not prepared
            String xid = sql.substring(sql.indexOf("'"), sql.length()).trim();
            testXid = true;
            ((MySQLConnection) conn).sendQueryCmd("xa start " + xid, conn.getCharset());
        } else if (errPg.getErrNo() == ErrorCode.ER_XAER_DUPID) {
            // ERROR 1440 (XAE08): XAER_DUPID: The XID already exists
            conn.close("test xid existence");
            doFinished(true);
        } else {
            conn.release();
            doFinished(true);
        }
    }

    @Override
    public void okResponse(byte[] ok, BackendConnection conn) {
        if (conn.syncAndExecute()) {
            if (testXid) {
                conn.closeWithoutRsp("test xid existence");
            } else {
                conn.release();
            }
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

    @Override
    public Object clone() {
        SQLJob newSqlJob = null;
        try {
            newSqlJob = (SQLJob) super.clone();
            newSqlJob.finished.set(false);
        } catch (CloneNotSupportedException e) {
            // ignore
            LOGGER.warn("SQLJob CloneNotSupportedException, impossible");
        }
        return newSqlJob;
    }

}
