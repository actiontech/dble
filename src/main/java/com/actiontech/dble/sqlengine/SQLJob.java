/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sqlengine;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.backend.datasource.ShardingNode;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.singleton.TraceManager;
import com.google.common.collect.ImmutableMap;
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
    private final String shardingNode;
    private final String schema;
    private BackendConnection connection;
    private final SQLJobHandler jobHandler;
    private final PhysicalDbInstance ds;
    private boolean isMustWriteNode = false;
    private AtomicBoolean finished = new AtomicBoolean(false);
    private volatile boolean testXid;

    public SQLJob(String sql, String schema, SQLJobHandler jobHandler, PhysicalDbInstance ds) {
        super();
        this.sql = sql;
        this.jobHandler = jobHandler;
        this.ds = ds;
        this.schema = schema;
        this.shardingNode = null;
    }

    public SQLJob(String sql, String shardingNode, SQLJobHandler jobHandler, boolean isMustWriteNode) {
        super();
        this.sql = sql;
        this.jobHandler = jobHandler;
        this.ds = null;
        this.shardingNode = shardingNode;
        this.schema = null;
        this.isMustWriteNode = isMustWriteNode;
    }

    public void run() {
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("sql-job-start");
        TraceManager.log(ImmutableMap.of("sql", sql), traceObject);
        try {
            if (ds == null) {
                RouteResultsetNode node = new RouteResultsetNode(shardingNode, ServerParse.SELECT, sql);
                // create new connection
                ShardingNode dn = DbleServer.getInstance().getConfig().getShardingNodes().get(node.getName());
                dn.getConnection(dn.getDatabase(), isMustWriteNode, true, node, this, node);
            } else {
                ds.getConnection(schema, this, null, isMustWriteNode);
            }
        } catch (Exception e) {
            LOGGER.warn("can't get connection", e);
            doFinished(true);
        } finally {
            TraceManager.finishSpan(traceObject);
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
        conn.getBackendService().setResponseHandler(this);
        conn.getBackendService().setComplexQuery(true);
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(conn.getBackendService(), "sql-job-send-command");
        try {
            conn.getBackendService().query(sql, true);
            connection = conn;
        } catch (Exception e) { // (UnsupportedEncodingException e) {
            LOGGER.debug("......", e);
            doFinished(true);
        } finally {
            TraceManager.finishSpan(traceObject);
        }

    }

    public boolean isFinished() {
        return finished.get();
    }

    protected boolean doFinished(boolean failed) {
        if (finished.compareAndSet(false, true)) {
            jobHandler.finished(shardingNode == null ? schema : shardingNode, failed);
            return true;
        }
        return false;
    }

    @Override
    public void connectionError(Throwable e, Object attachment) {
        LOGGER.warn("can't get connection for sql :" + sql, e);
        doFinished(true);
    }

    @Override
    public void errorResponse(byte[] err, AbstractService service) {
        ErrorPacket errPg = new ErrorPacket();
        errPg.read(err);

        String errMsg = "error response errNo:" + errPg.getErrNo() + ", " + new String(errPg.getMessage()) +
                " from of sql :" + sql + " at con:" + service;

        LOGGER.info(errMsg);
        if (!((MySQLResponseService) service).syncAndExecute()) {
            service.getConnection().businessClose("unfinished sync");
            doFinished(true);
            return;
        }

        if (errPg.getErrNo() == ErrorCode.ER_XAER_NOTA) {
            // ERROR 1397 (XAE04): XAER_NOTA: Unknown XID, not prepared
            String xid = sql.substring(sql.indexOf("'"), sql.length()).trim();
            testXid = true;
            ((MySQLResponseService) service).sendQueryCmd("xa start " + xid, service.getConnection().getCharsetName());
        } else if (errPg.getErrNo() == ErrorCode.ER_XAER_DUPID) {
            // ERROR 1440 (XAE08): XAER_DUPID: The XID already exists
            service.getConnection().close("test xid existence");
            doFinished(true);
        } else {
            ((MySQLResponseService) service).release();
            doFinished(true);
        }
    }

    @Override
    public void okResponse(byte[] ok, AbstractService service) {
        if (((MySQLResponseService) service).syncAndExecute()) {
            if (testXid) {
                service.getConnection().businessClose("test xid existence");
            } else {
                ((MySQLResponseService) service).release();
            }
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
        ((MySQLResponseService) service).release();
        doFinished(false);
    }

    @Override
    public void connectionClose(AbstractService service, String reason) {
        doFinished(true);
    }

    @Override
    public String toString() {
        return "SQLJob [shardingNode=" +
                shardingNode + ",schema=" +
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
