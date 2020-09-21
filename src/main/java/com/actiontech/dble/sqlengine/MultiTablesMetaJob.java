/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sqlengine;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.backend.datasource.ShardingNode;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.meta.ReloadLogHelper;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * async execute in EngineCtx or standalone (EngineCtx=null)
 *
 * @author yhq
 */
public class MultiTablesMetaJob implements ResponseHandler, Runnable {

    public final ReloadLogHelper logger;

    private final String sql;
    private final String shardingNode;
    private final String schema;
    private BackendConnection connection;
    private final SQLJobHandler jobHandler;
    private final PhysicalDbInstance ds;
    private boolean isMustWriteNode;
    private AtomicBoolean finished = new AtomicBoolean(false);

    public MultiTablesMetaJob(String sql, String schema, SQLJobHandler jobHandler, PhysicalDbInstance ds, boolean isReload) {
        super();
        this.logger = new ReloadLogHelper(isReload);
        this.sql = sql;
        this.jobHandler = jobHandler;
        this.ds = ds;
        this.schema = schema;
        this.shardingNode = null;
    }

    public MultiTablesMetaJob(String sql, String shardingNode, SQLJobHandler jobHandler, boolean isMustWriteNode, boolean isReload) {
        super();
        this.sql = sql;
        this.jobHandler = jobHandler;
        this.ds = null;
        this.shardingNode = shardingNode;
        this.schema = null;
        this.isMustWriteNode = isMustWriteNode;
        this.logger = new ReloadLogHelper(isReload);
    }

    public void run() {
        try {
            if (ds == null) {
                RouteResultsetNode node = new RouteResultsetNode(shardingNode, ServerParse.SELECT, sql);
                // create new connection
                ShardingNode dn = DbleServer.getInstance().getConfig().getShardingNodes().get(node.getName());
                dn.getConnection(dn.getDatabase(), isMustWriteNode, true, node, this, node);
            } else {
                ds.getConnection(schema, this, null, false);
            }
        } catch (Exception e) {
            logger.warn("can't get connection" + shardingNode, e);
            doFinished(true);
        }
    }

    public void terminate(String reason) {
        logger.info("terminate this job reason:" + reason + " con:" + connection + " sql " + this.sql);
        if (connection != null) {
            connection.close(reason);
        }
    }

    @Override
    public void connectionAcquired(final BackendConnection conn) {
        conn.getBackendService().setResponseHandler(this);
        conn.getBackendService().setComplexQuery(true);
        logger.info("connectionAcquired on connection " + conn.getBackendService());
        try {
            conn.getBackendService().query(sql, true);
            connection = conn;
        } catch (Exception e) { // (UnsupportedEncodingException e) {
            doFinished(true);
        }

    }

    public boolean isFinished() {
        return finished.get();
    }

    private void doFinished(boolean failed) {
        logger.info("Finish MultiTablesMetaJob with result " + failed + " on connection " + connection);
        if (finished.compareAndSet(false, true)) {
            jobHandler.finished(shardingNode == null ? schema : shardingNode, failed);
        }
    }

    @Override
    public void connectionError(Throwable e, Object attachment) {
        logger.warn("can't get connection for sql :" + sql, e);
        doFinished(true);
    }

    @Override
    public void errorResponse(byte[] err, AbstractService service) {
        ErrorPacket errPg = new ErrorPacket();
        errPg.read(err);

        String errMsg = "error response errNo:" + errPg.getErrNo() + ", " + new String(errPg.getMessage()) +
                " from of sql :" + sql + " at con:" + service;

        logger.info(errMsg);
        if (((MySQLResponseService) service).syncAndExecute()) {
            ((MySQLResponseService) service).release();
        } else {
            ((MySQLResponseService) service).getConnection().businessClose("unfinished sync");
        }
        doFinished(true);
    }

    @Override
    public void okResponse(byte[] ok, AbstractService service) {
        if (((MySQLResponseService) service).syncAndExecute()) {
            ((MySQLResponseService) service).release();
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
        EOFPacket packet = new EOFPacket();
        packet.read(eof);
        if ((packet.getStatus() & StatusFlags.SERVER_MORE_RESULTS_EXISTS) <= 0) {
            ((MySQLResponseService) service).getConnection().release();
            doFinished(false);
        }
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

}
