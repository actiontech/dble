package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.DDLTraceInfo;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.singleton.DDLTraceManager;
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by szf on 2019/12/3.
 */
public class SingleNodeDDLHandler extends SingleNodeHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(SingleNodeDDLHandler.class);

    private AtomicBoolean specialHandleFlag = new AtomicBoolean(false); // execute special handling only once

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
    public void connectionError(Throwable e, BackendConnection conn) {
        DDLTraceManager.getInstance().updateConnectionStatus(session.getSource(),
                (MySQLConnection) conn, DDLTraceInfo.DDLConnectionStatus.EXECUTE_CONN_ERROR);
        DDLTraceManager.getInstance().endDDL(session.getSource(), e.getMessage());
        super.connectionError(e, conn);
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
            DDLTraceManager.getInstance().updateDDLStatus(DDLTraceInfo.DDLStage.META_UPDATE, session.getSource());

            //handleSpecial
            boolean metaInited = handleSpecial(rrs, true);
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
                if (writeToClient.compareAndSet(false, true)) {
                    handleEndPacket(ok.toBytes(), true);
                }
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
            handleEndPacket(errPacket.toBytes(), false);
        }
        session.multiStatementNextSql(multiStatementFlag);
    }

    protected void backConnectionErr(ErrorPacket errPkg, BackendConnection conn, boolean syncFinished) {
        ServerConnection source = session.getSource();
        String errUser = source.getUser();
        String errHost = source.getHost();
        int errPort = source.getLocalPort();

        String errMsg = " errNo:" + errPkg.getErrNo() + " " + new String(errPkg.getMessage());
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
        source.setTxInterrupt(errMsg);
        handleSpecial(rrs, false);
        lock.lock();
        try {
            if (writeToClient.compareAndSet(false, true)) {
                if (buffer != null) {
                    /* SELECT 9223372036854775807 + 1;    response: field_count, field, eof, err */
                    buffer = source.writeToBuffer(errPkg.toBytes(), buffer);
                    session.setResponseTime(false);
                    handleEndPacket(buffer, false);
                } else {
                    handleEndPacket(errPkg.toBytes(), false);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public boolean clearIfSessionClosed() {
        if (session.closed()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("session closed without execution,clear resources " + session);
            }
            handleSpecial(rrs, false);
            session.clearResources(true);
            recycleBuffer();
            return true;
        } else {
            return false;
        }
    }

    private boolean handleSpecial(RouteResultset rrs0, boolean isSuccess) {
        if (specialHandleFlag.compareAndSet(false, true)) {
            return session.handleSpecial(rrs0, isSuccess, null);
        }
        return true;
    }

    private void handleEndPacket(Object obj, boolean isSuccess) {
        session.clearResources(false);
        session.setResponseTime(isSuccess);
        if (obj instanceof byte[]) {
            session.getSource().write((byte[]) obj);
        } else if (obj instanceof ByteBuffer) {
            session.getSource().write((ByteBuffer) obj);
        }
        if (isSuccess)
            session.multiStatementNextSql(session.getIsMultiStatement().get());
    }
}
