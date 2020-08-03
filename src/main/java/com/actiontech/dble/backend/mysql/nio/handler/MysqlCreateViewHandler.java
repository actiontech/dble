/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.datasource.ShardingNode;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.meta.ViewMeta;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @Author collapsar
 */
public class MysqlCreateViewHandler implements ResponseHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlCreateViewHandler.class);
    private NonBlockingSession session;
    private RouteResultset rrs;
    private volatile byte packetId;
    private ViewMeta vm;

    public MysqlCreateViewHandler(NonBlockingSession session, RouteResultset rrs, ViewMeta vm) {
        this.session = session;
        this.rrs = rrs;
        this.packetId = (byte) session.getPacketId().get();
        this.vm = vm;
    }

    public void execute() throws Exception {
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

    private void innerExecute(BackendConnection conn, RouteResultsetNode node) {
        conn.setResponseHandler(this);
        conn.setSession(session);
        conn.execute(node, session.getSource(), session.getSource().isAutocommit());
    }

    @Override
    public void connectionAcquired(BackendConnection conn) {
        final RouteResultsetNode node = (RouteResultsetNode) conn.getAttachment();
        session.bindConnection(node, conn);
        innerExecute(conn, node);
    }

    @Override
    public void connectionError(Throwable e, BackendConnection conn) {
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.setPacketId(++packetId);
        errPacket.setErrNo(ErrorCode.ER_DB_INSTANCE_ABORTING_CONNECTION);
        String errMsg = "Backend connect Error, Connection{dbInstance[" + conn.getHost() + ":" + conn.getPort() + "],Schema[" + conn.getSchema() + "]} refused";
        errPacket.setMessage(StringUtil.encode(errMsg, session.getSource().getCharset().getResults()));
        backConnectionErr(errPacket, conn, conn.syncAndExecute());
    }

    @Override
    public void errorResponse(byte[] data, BackendConnection conn) {
        ErrorPacket errPkg = new ErrorPacket();
        errPkg.read(data);
        errPkg.setPacketId(++packetId);
        backConnectionErr(errPkg, conn, conn.syncAndExecute());
    }

    @Override
    public void okResponse(byte[] data, BackendConnection conn) {
        boolean executeResponse = conn.syncAndExecute();
        if (!executeResponse) {
            return;
        }

        try {
            vm.addMeta(true);
        } catch (Exception e) {
            ErrorPacket errPkg = new ErrorPacket();
            errPkg.setPacketId(++packetId);
            errPkg.setMessage(StringUtil.encode(e.getMessage(), session.getSource().getCharset().getResults()));
            backConnectionErr(errPkg, conn, conn.syncAndExecute());
            return;
        }

        // return ok
        OkPacket ok = new OkPacket();
        ok.read(data);
        ok.setPacketId(++packetId); // OK_PACKET
        ok.setServerStatus(session.getSource().isAutocommit() ? 2 : 1);
        session.setBackendResponseEndTime((MySQLConnection) conn);
        session.releaseConnectionIfSafe(conn, false);
        session.setResponseTime(true);
        session.multiStatementPacket(ok, packetId);
        boolean multiStatementFlag = session.getIsMultiStatement().get();
        ok.write(session.getSource());
        session.multiStatementNextSql(multiStatementFlag);
    }

    private void backConnectionErr(ErrorPacket errPkg, BackendConnection conn, boolean syncFinished) {
        ServerConnection source = session.getSource();
        Pair<String, String> errUser = source.getUser();
        String errHost = source.getHost();
        int errPort = source.getLocalPort();

        String errMsg = " errNo:" + errPkg.getErrNo() + " " + new String(errPkg.getMessage());
        LOGGER.info("execute sql err :" + errMsg + " con:" + conn +
                " frontend host:" + errHost + "/" + errPort + "/" + errUser);

        if (syncFinished) {
            session.releaseConnectionIfSafe(conn, false);
        } else {
            conn.closeWithoutRsp("unfinished sync");
            session.getTargetMap().remove(conn.getAttachment());
        }
        source.setTxInterrupt(errMsg);
        errPkg.write(source);
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof, boolean isLeft, BackendConnection conn) {
        //not happen
    }

    @Override
    public boolean rowResponse(byte[] rowNull, RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        //not happen
        return false;
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, BackendConnection conn) {
        //not happen
    }

    @Override
    public void connectionClose(BackendConnection conn, String reason) {
        //not happen
    }

}
