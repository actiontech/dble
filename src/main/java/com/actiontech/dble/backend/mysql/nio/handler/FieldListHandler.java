package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.datasource.ShardingNode;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.user.UserName;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.singleton.ProxyMeta;
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.SQLNonTransientException;
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
            ShardingNode dn = DbleServer.getInstance().getConfig().getShardingNodes().get(node.getName());
            dn.getConnection(dn.getDatabase(), session.getSource().isTxStart(), session.getSource().isAutocommit(), node, this, node);
        }
    }

    @Override
    public void connectionError(Throwable e, Object attachment) {
        RouteResultsetNode rrn = (RouteResultsetNode) attachment;
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.setPacketId(++packetId);
        errPacket.setErrNo(ErrorCode.ER_DB_INSTANCE_ABORTING_CONNECTION);
        String errMsg = "can't connect to shardingNode[" + rrn.getName() + "], due to " + e.getMessage();
        errPacket.setMessage(StringUtil.encode(errMsg, session.getSource().getCharset().getResults()));
        LOGGER.warn(errMsg);
        backConnectionErr(errPacket, null, false);
    }

    @Override
    public void connectionAcquired(BackendConnection conn) {
        final RouteResultsetNode node = (RouteResultsetNode) conn.getAttachment();
        session.bindConnection(node, conn);
        innerExecute(conn, node);
    }

    @Override
    public void errorResponse(byte[] data, BackendConnection conn) {
        ErrorPacket errPkg = new ErrorPacket();
        errPkg.read(data);
        errPkg.setPacketId(++packetId);
        backConnectionErr(errPkg, conn, conn.syncAndExecute());
    }

    @Override
    public void okResponse(byte[] ok, BackendConnection conn) {
        //not happen
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPacketsNull, byte[] eof, boolean isLeft, BackendConnection conn) {
        ServerConnection source = session.getSource();
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
            } catch (SQLNonTransientException e) {
                LOGGER.warn("field list response use default value because of Meta don't exist:schema[{}],table[{}]", rrs.getSchema(), orgTable, e);
                fieldPk.setDefaultVal(FieldPacket.DEFAULT_VALUE);
            }
            buffer = fieldPk.write(buffer, source, false);
        }
        eof[3] = ++packetId;
        buffer = source.writeToBuffer(eof, buffer);
    }

    @Override
    public boolean rowResponse(byte[] rowNull, RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        //not happen
        return false;
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, BackendConnection conn) {
        session.getSource().write(buffer);
    }

    @Override
    public void connectionClose(BackendConnection conn, String reason) {
        if (connClosed) {
            return;
        }
        connClosed = true;
        LOGGER.warn("Backend connect Closed, reason is [" + reason + "], Connection info:" + conn);
        reason = "Connection {dbInstance[" + conn.getHost() + ":" + conn.getPort() + "],Schema[" + conn.getSchema() + "],threadID[" +
                ((MySQLConnection) conn).getThreadId() + "]} was closed ,reason is [" + reason + "]";
        ErrorPacket err = new ErrorPacket();
        err.setPacketId(++packetId);
        err.setErrNo(ErrorCode.ER_ERROR_ON_CLOSE);
        err.setMessage(StringUtil.encode(reason, session.getSource().getCharset().getResults()));
        backConnectionErr(err, conn, false);
    }

    private void backConnectionErr(ErrorPacket errPkg, BackendConnection conn, boolean syncFinished) {
        ServerConnection source = session.getSource();
        UserName errUser = source.getUser();
        String errHost = source.getHost();
        int errPort = source.getLocalPort();
        String errMsg = " errNo:" + errPkg.getErrNo() + " " + new String(errPkg.getMessage());
        if (conn != null) {
            LOGGER.info("execute sql err :" + errMsg + " con:" + conn +
                    " frontend host:" + errHost + "/" + errPort + "/" + errUser);
            if (conn.isClosed()) {
                if (conn.getAttachment() != null) {
                    RouteResultsetNode rNode = (RouteResultsetNode) conn.getAttachment();
                    session.getTargetMap().remove(rNode);
                }
            } else if (syncFinished) {
                session.releaseConnectionIfSafe(conn, false);
            } else {
                conn.closeWithoutRsp("unfinished sync");
                if (conn.getAttachment() != null) {
                    RouteResultsetNode rNode = (RouteResultsetNode) conn.getAttachment();
                    session.getTargetMap().remove(rNode);
                }
            }
        }
        source.setTxInterrupt(errMsg);
        if (session.closed()) {
            recycleBuffer();
        }
        errPkg.write(source);
    }

    private void innerExecute(BackendConnection conn, RouteResultsetNode node) {
        if (session.closed()) {
            session.clearResources(true);
            recycleBuffer();
            return;
        }
        conn.setResponseHandler(this);
        conn.setSession(session);
        conn.execute(node, session.getSource(), session.getSource().isAutocommit());
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
