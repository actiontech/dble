package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.DDLTraceInfo;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.singleton.DDLTraceManager;

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
        DDLTraceManager.getInstance().updateConnectionStatus(session.getSource(), (MySQLConnection) conn, DDLTraceInfo.DDLConnectionStatus.EXECUTE_START);
        super.execute(conn);
    }


    @Override
    public void connectionError(Throwable e, BackendConnection conn) {
        DDLTraceManager.getInstance().updateConnectionStatus(session.getSource(),
                (MySQLConnection) conn, DDLTraceInfo.DDLConnectionStatus.EXECUTE_CONN_ERROR);
        DDLTraceManager.getInstance().endDDL(session.getSource(), e.getMessage());
        super.connectionError(e, conn);
    }


    @Override
    public void errorResponse(byte[] data, BackendConnection conn) {
        DDLTraceManager.getInstance().updateConnectionStatus(session.getSource(),
                (MySQLConnection) conn, DDLTraceInfo.DDLConnectionStatus.EXECUTE_ERROR);
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
            DDLTraceManager.getInstance().updateConnectionStatus(session.getSource(), (MySQLConnection) conn, DDLTraceInfo.DDLConnectionStatus.EXECUTE_SUCCESS);
            DDLTraceManager.getInstance().updateDDLStatus(DDLTraceInfo.DDLStage.META_UPDATE, session.getSource());
            //handleSpecial
            boolean metaInited = session.handleSpecial(rrs, true);
            if (!metaInited) {
                DDLTraceManager.getInstance().endDDL(session.getSource(), "ddl end with meta failure");
                executeMetaDataFailed(conn);
                return;
            } else {
                DDLTraceManager.getInstance().endDDL(session.getSource(), null);
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

}
