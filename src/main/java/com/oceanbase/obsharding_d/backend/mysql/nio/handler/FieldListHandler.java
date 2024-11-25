/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.nio.handler;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.datasource.ShardingNode;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.net.connection.BackendConnection;
import com.oceanbase.obsharding_d.net.mysql.ErrorPacket;
import com.oceanbase.obsharding_d.net.mysql.FieldPacket;
import com.oceanbase.obsharding_d.net.mysql.RowDataPacket;
import com.oceanbase.obsharding_d.net.service.AbstractService;
import com.oceanbase.obsharding_d.net.service.WriteFlags;
import com.oceanbase.obsharding_d.route.RouteResultset;
import com.oceanbase.obsharding_d.route.RouteResultsetNode;
import com.oceanbase.obsharding_d.server.NonBlockingSession;
import com.oceanbase.obsharding_d.services.mysqlsharding.MySQLResponseService;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;
import com.oceanbase.obsharding_d.singleton.ProxyMeta;
import com.oceanbase.obsharding_d.util.StringUtil;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class FieldListHandler implements ResponseHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(FieldListHandler.class);
    private NonBlockingSession session;
    private RouteResultset rrs;
    private ReentrantLock lock = new ReentrantLock();
    private volatile byte packetId;
    private volatile ByteBuffer buffer;
    private volatile boolean connClosed = false;

    public FieldListHandler(NonBlockingSession session, RouteResultset rrs) {
        this.session = session;
        this.rrs = rrs;
        this.packetId = (byte) session.getPacketId().get();
    }

    public void execute() throws Exception {
        connClosed = false;
        RouteResultsetNode node = rrs.getNodes()[0];
        BackendConnection conn = session.getTarget(node);
        if (session.tryExistsCon(conn, node)) {
            innerExecute(conn, node);
        } else {
            // create new connection
            ShardingNode dn = OBsharding_DServer.getInstance().getConfig().getShardingNodes().get(node.getName());
            dn.getConnection(dn.getDatabase(), session.getShardingService().isTxStart(), session.getShardingService().isAutocommit(), node, this, node);
        }
    }

    @Override
    public void connectionError(Throwable e, Object attachment) {
        RouteResultsetNode rrn = (RouteResultsetNode) attachment;
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.setPacketId(++packetId);
        errPacket.setErrNo(ErrorCode.ER_DB_INSTANCE_ABORTING_CONNECTION);
        String errMsg = "can't connect to shardingNode[" + rrn.getName() + "], due to " + e.getMessage();
        errPacket.setMessage(StringUtil.encode(errMsg, session.getSource().getService().getCharset().getResults()));
        LOGGER.warn(errMsg);
        backConnectionErr(errPacket, null, false);
    }

    @Override
    public void connectionAcquired(BackendConnection conn) {
        final RouteResultsetNode node = (RouteResultsetNode) conn.getBackendService().getAttachment();
        session.bindConnection(node, conn);
        innerExecute(conn, node);
    }

    @Override
    public void errorResponse(byte[] data, @NotNull AbstractService service) {
        ErrorPacket errPkg = new ErrorPacket();
        errPkg.read(data);
        errPkg.setPacketId(++packetId);
        backConnectionErr(errPkg, (MySQLResponseService) service, ((MySQLResponseService) service).syncAndExecute());
    }

    @Override
    public void okResponse(byte[] ok, @NotNull AbstractService service) {
        ((MySQLResponseService) service).syncAndExecute();
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPacketsNull, byte[] eof, boolean isLeft, @NotNull AbstractService service) {
        ShardingService shardingService = session.getShardingService();
        buffer = session.getSource().allocate();
        for (int i = 0, len = fields.size(); i < len; ++i) {
            byte[] field = fields.get(i);
            field[3] = ++packetId;

            // save field
            FieldPacket fieldPk = new FieldPacket();
            fieldPk.read(field);
            if (rrs.getSchema() != null) {
                fieldPk.setDb(rrs.getSchema().getBytes());
            }
            if (rrs.getTableAlias() != null) {
                fieldPk.setTable(rrs.getTableAlias().getBytes());
            }
            if (rrs.getTable() != null) {
                fieldPk.setOrgTable(rrs.getTable().getBytes());
            }
            //update default
            String orgName = new String(fieldPk.getOrgName()).toUpperCase();
            String orgTable = new String(fieldPk.getOrgTable());
            try {
                boolean isTable = null != ProxyMeta.getInstance().getTmManager().getSyncTableMeta(rrs.getSchema(), orgTable);
                String defaultVal = null;
                if (isTable) {
                    defaultVal = ProxyMeta.getInstance().getTmManager().getSyncTableMeta(rrs.getSchema(), orgTable).getColumns().stream().filter(t -> orgName.equalsIgnoreCase(t.getName())).findFirst().get().getDefaultVal();
                }
                fieldPk.setDefaultVal(null != defaultVal ? defaultVal.getBytes() : FieldPacket.DEFAULT_VALUE);
            } catch (Exception e) {
                LOGGER.warn("field list response use default value because of Meta don't exist:schema[{}],table[{}]", rrs.getSchema(), orgTable, e);
                fieldPk.setDefaultVal(FieldPacket.DEFAULT_VALUE);
            }
            buffer = fieldPk.write(buffer, shardingService, false);
        }
        eof[3] = ++packetId;
        buffer = shardingService.writeToBuffer(eof, buffer);
    }

    @Override
    public boolean rowResponse(byte[] rowNull, RowDataPacket rowPacket, boolean isLeft, @NotNull AbstractService service) {
        //not happen
        return false;
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, @NotNull AbstractService service) {
        session.releaseConnectionIfSafe((MySQLResponseService) service, false);
        session.getShardingService().writeDirectly(buffer, WriteFlags.QUERY_END);
    }

    @Override
    public void connectionClose(@NotNull AbstractService service, String reason) {
        if (connClosed) {
            return;
        }
        connClosed = true;
        LOGGER.warn("Backend connect Closed, reason is [" + reason + "], Connection info:" + service);
        reason = "Connection {dbInstance[" + service.getConnection().getHost() + ":" + service.getConnection().getPort() + "],Schema[" + ((MySQLResponseService) service).getConnection().getSchema() + "],threadID[" +
                ((MySQLResponseService) service).getConnection().getThreadId() + "]} was closed ,reason is [" + reason + "]";
        ErrorPacket err = new ErrorPacket();
        err.setPacketId(++packetId);
        err.setErrNo(ErrorCode.ER_ERROR_ON_CLOSE);
        err.setMessage(StringUtil.encode(reason, session.getSource().getService().getCharset().getResults()));
        backConnectionErr(err, (MySQLResponseService) service, false);
    }

    private void backConnectionErr(ErrorPacket errPkg, @Nullable MySQLResponseService responseService, boolean syncFinished) {
        ShardingService shardingService = session.getShardingService();
        String errMsg = "errNo:" + errPkg.getErrNo() + " " + new String(errPkg.getMessage());
        if (responseService != null && !responseService.isFakeClosed()) {
            LOGGER.info("execute sql err:{}, con:{}", errMsg, responseService);

            if (responseService.getConnection().isClosed()) {
                if (responseService.getAttachment() != null) {
                    RouteResultsetNode rNode = (RouteResultsetNode) responseService.getAttachment();
                    session.getTargetMap().remove(rNode);
                }
            } else if (syncFinished) {
                session.releaseConnectionIfSafe(responseService, false);
            } else {
                responseService.getConnection().businessClose("unfinished sync");
                if (responseService.getAttachment() != null) {
                    RouteResultsetNode rNode = (RouteResultsetNode) responseService.getAttachment();
                    session.getTargetMap().remove(rNode);
                }
            }
        }
        shardingService.setTxInterrupt(errMsg);
        if (session.closed()) {
            recycleBuffer();
        }
        errPkg.write(shardingService.getConnection());
    }

    private void innerExecute(BackendConnection conn, RouteResultsetNode node) {
        if (session.closed()) {
            session.clearResources(true);
            recycleBuffer();
            return;
        }
        conn.getBackendService().setResponseHandler(this);
        conn.getBackendService().setSession(session);
        conn.getBackendService().execute(node, session.getShardingService(), session.getShardingService().isAutocommit());
    }

    public void recycleBuffer() {
        lock.lock();
        try {
            if (buffer != null) {
                session.getSource().recycle(buffer);
                buffer = null;
            }
        } finally {
            lock.unlock();
        }
    }
}
