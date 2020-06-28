package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.cluster.zkprocess.zookeeper.process.DDLTraceInfo;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.singleton.DDLTraceManager;
import com.actiontech.dble.util.StringUtil;

/**
 * Created by szf on 2019/12/3.
 */
public class SingleNodeDDLHandler extends SingleNodeHandler {

    public SingleNodeDDLHandler(RouteResultset rrs, NonBlockingSession session) {
        super(rrs, session);
    }

    public void execute() throws Exception {
        DDLTraceManager.getInstance().updateDDLStatus(DDLTraceInfo.DDLStage.EXECUTE_START, session.getSource());
        try {
            super.execute();
        } catch (Exception e) {
            DDLTraceManager.getInstance().endDDL(session.getSource(), e.getMessage());
            throw e;
        }
    }

    public void execute(BackendConnection conn) {
        DDLTraceManager.getInstance().updateConnectionStatus(session.getSource(), (MySQLConnection) conn, DDLTraceInfo.DDLConnectionStatus.CONN_EXECUTE_START);
        super.execute(conn);
    }

    @Override
    public void connectionError(Throwable e, Object attachment) {
        DDLTraceManager.getInstance().updateRouteNodeStatus(session.getSource(),
                (RouteResultsetNode) attachment, DDLTraceInfo.DDLConnectionStatus.TEST_CONN_ERROR);
        DDLTraceManager.getInstance().endDDL(session.getSource(), e.getMessage());
        super.connectionError(e, attachment);
    }


    @Override
    public void errorResponse(byte[] data, BackendConnection conn) {
        DDLTraceManager.getInstance().updateConnectionStatus(session.getSource(),
                (MySQLConnection) conn, DDLTraceInfo.DDLConnectionStatus.CONN_EXECUTE_ERROR);
        DDLTraceManager.getInstance().endDDL(session.getSource(), "ddl end with execution failure");
        super.errorResponse(data, conn);
    }

    @Override
    public void connectionClose(BackendConnection conn, String reason) {
        DDLTraceManager.getInstance().updateConnectionStatus(session.getSource(),
                (MySQLConnection) conn, DDLTraceInfo.DDLConnectionStatus.EXECUTE_CONN_CLOSE);
        DDLTraceManager.getInstance().endDDL(session.getSource(), reason);
        super.connectionClose(conn, reason);
    }


    @Override
    public void okResponse(byte[] data, BackendConnection conn) {
        boolean executeResponse = conn.syncAndExecute();
        if (executeResponse) {
            DDLTraceManager.getInstance().updateConnectionStatus(session.getSource(), (MySQLConnection) conn, DDLTraceInfo.DDLConnectionStatus.CONN_EXECUTE_SUCCESS);
            // handleSpecial
            boolean metaInitial = session.handleSpecial(rrs, true, null);
            if (!metaInitial) {
                DDLTraceManager.getInstance().endDDL(session.getSource(), "ddl end with meta failure");
                executeMetaDataFailed(conn);
            } else {
                DDLTraceManager.getInstance().endDDL(session.getSource(), null);
                session.setRowCount(0);
                ServerConnection source = session.getSource();
                OkPacket ok = new OkPacket();
                ok.read(data);
                ok.setPacketId(++packetId); // OK_PACKET
                ok.setMessage(null);
                ok.setServerStatus(source.isAutocommit() ? 2 : 1);
                source.setLastInsertId(ok.getInsertId());
                session.setBackendResponseEndTime((MySQLConnection) conn);
                session.releaseConnectionIfSafe(conn, false);
                session.setResponseTime(true);
                session.multiStatementPacket(ok, packetId);
                boolean multiStatementFlag = session.getIsMultiStatement().get();
                if (writeToClient.compareAndSet(false, true)) {
                    ok.write(source);
                }
                session.multiStatementNextSql(multiStatementFlag);
            }
        }
    }

    private void executeMetaDataFailed(BackendConnection conn) {
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.setPacketId(++packetId);
        errPacket.setErrNo(ErrorCode.ER_META_DATA);
        String errMsg = "Create TABLE OK, but generate metedata failed. The reason may be that the current druid parser can not recognize part of the sql" +
                " or the user for backend mysql does not have permission to execute the heartbeat sql.";
        errPacket.setMessage(StringUtil.encode(errMsg, session.getSource().getCharset().getResults()));

        session.setBackendResponseEndTime((MySQLConnection) conn);
        session.releaseConnectionIfSafe(conn, false);
        session.setResponseTime(false);
        session.multiStatementPacket(errPacket, packetId);
        boolean multiStatementFlag = session.getIsMultiStatement().get();
        doSqlStat();
        if (writeToClient.compareAndSet(false, true)) {
            errPacket.write(session.getSource());
        }
        session.multiStatementNextSql(multiStatementFlag);
    }

    @Override
    protected void backConnectionErr(ErrorPacket errPkg, BackendConnection conn, boolean syncFinished) {
        ServerConnection source = session.getSource();
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
        String errMsg = " errNo:" + errPkg.getErrNo() + " " + new String(errPkg.getMessage());
        source.setTxInterrupt(errMsg);
        if (writeToClient.compareAndSet(false, true)) {
            session.handleSpecial(rrs, false, null);
            errPkg.write(source);
        }
    }

}
