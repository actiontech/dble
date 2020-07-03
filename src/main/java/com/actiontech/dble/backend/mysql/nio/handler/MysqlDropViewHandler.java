/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.ShardingNode;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.user.UserName;
import com.actiontech.dble.meta.ViewMeta;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;

import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLNonTransientException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author collapsar
 */
public class MysqlDropViewHandler implements ResponseHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlDropViewHandler.class);
    private NonBlockingSession session;
    private RouteResultset rrs;
    private volatile byte packetId;
    private AtomicInteger viewNum;
    private ViewMeta vm; //if only for replace from a no sharding view to a sharding view

    public MysqlDropViewHandler(NonBlockingSession session, RouteResultset rrs, int viewNum) {
        this.session = session;
        this.rrs = rrs;
        this.packetId = (byte) session.getPacketId().get();
        this.viewNum = new AtomicInteger(viewNum);
    }

    public void execute() throws Exception {
        RouteResultsetNode node = rrs.getNodes()[0];
        BackendConnection conn = session.getTarget(node);
        if (session.tryExistsCon(conn, node)) {
            innerExecute(conn, node);
        } else {
            // create new connection
            ShardingNode dn = DbleServer.getInstance().getConfig().getShardingNodes().get(node.getName());
            dn.getConnection(dn.getDatabase(), session.getShardingService().isTxStart(), session.getShardingService().isAutocommit(), node, this, node);
        }
    }

    public void setVm(ViewMeta vm) {
        this.vm = vm;
    }

    private void innerExecute(BackendConnection conn, RouteResultsetNode node) {
        conn.getBackendService().setResponseHandler(this);
        conn.getBackendService().setSession(session);
        conn.getBackendService().execute(node, session.getShardingService(), session.getShardingService().isAutocommit());
    }

    @Override
    public void connectionAcquired(BackendConnection conn) {
        final RouteResultsetNode node = (RouteResultsetNode) conn.getBackendService().getAttachment();
        session.bindConnection(node, conn);
        innerExecute(conn, node);
    }

    @Override
    public void connectionError(Throwable e, Object attachment) {
        RouteResultsetNode rrn = (RouteResultsetNode) attachment;
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.setPacketId(++packetId);
        errPacket.setErrNo(ErrorCode.ER_DB_INSTANCE_ABORTING_CONNECTION);
        String errMsg = "can't connect to shardingNode[" + rrn.getName() + "], due to " + e.getMessage();
        errPacket.setMessage(StringUtil.encode(errMsg, session.getShardingService().getCharset().getResults()));
        LOGGER.warn(errMsg);
        backConnectionErr(errPacket, null, false);
    }

    @Override
    public void errorResponse(byte[] data, AbstractService service) {
        ErrorPacket errPkg = new ErrorPacket();
        errPkg.read(data);
        errPkg.setPacketId(++packetId);
        backConnectionErr(errPkg, (MySQLResponseService) service, ((MySQLResponseService) service).syncAndExecute());
    }

    @Override
    public void okResponse(byte[] data, AbstractService service) {
        boolean executeResponse = ((MySQLResponseService) service).syncAndExecute();
        if (!executeResponse) {
            return;
        }

        if (viewNum.decrementAndGet() == 0) {
            if (vm != null) { // replace a new sharding view
                try {
                    vm.addMeta(true);
                } catch (SQLNonTransientException e) {
                    ErrorPacket errPkg = new ErrorPacket();
                    errPkg.setPacketId(++packetId);
                    errPkg.setMessage(StringUtil.encode(e.getMessage(), session.getShardingService().getCharset().getResults()));
                    backConnectionErr(errPkg, ((MySQLResponseService) service), ((MySQLResponseService) service).syncAndExecute());
                    return;
                }
            }

            // return ok
            OkPacket ok = new OkPacket();
            ok.read(data);
            ok.setPacketId(++packetId); // OK_PACKET
            ok.setServerStatus(session.getShardingService().isAutocommit() ? 2 : 1);
            session.setBackendResponseEndTime(((MySQLResponseService) service));
            session.releaseConnectionIfSafe(((MySQLResponseService) service), false);
            session.setResponseTime(true);
            ok.write(session.getSource());
        }
    }

    private void backConnectionErr(ErrorPacket errPkg, MySQLResponseService service, boolean syncFinished) {
        ShardingService shardingService = session.getShardingService();
        UserName errUser = shardingService.getUser();
        String errHost = shardingService.getConnection().getHost();
        int errPort = shardingService.getConnection().getLocalPort();
        String errMsg = " errNo:" + errPkg.getErrNo() + " " + new String(errPkg.getMessage());
        if (service != null) {
            LOGGER.info("execute sql err :" + errMsg + " con:" + service +
                    " frontend host:" + errHost + "/" + errPort + "/" + errUser);
            if (syncFinished) {
                session.releaseConnectionIfSafe(service, false);
            } else {
                service.getConnection().businessClose("unfinished sync");
                session.getTargetMap().remove(service.getAttachment());
            }
        }

        if (viewNum.decrementAndGet() == 0) {
            shardingService.setTxInterrupt(errMsg);
            errPkg.write(shardingService.getConnection());
        }
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof, boolean isLeft, AbstractService service) {
        //not happen
    }

    @Override
    public boolean rowResponse(byte[] rowNull, RowDataPacket rowPacket, boolean isLeft, AbstractService service) {
        //not happen
        return false;
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, AbstractService service) {
        //not happen
    }

    @Override
    public void connectionClose(AbstractService service, String reason) {
        //not happen
    }

}
