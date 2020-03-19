/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.datasource.PhysicalDataNode;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.DDLTraceInfo;
import com.actiontech.dble.log.transaction.TxnLogHelper;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.singleton.DDLTraceManager;
import com.actiontech.dble.util.FormatUtil;
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;

/**
 * @author mycat
 */
public class MultiNodeDDLExecuteHandler extends MultiNodeQueryHandler implements LoadDataResponseHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiNodeQueryHandler.class);

    public MultiNodeDDLExecuteHandler(RouteResultset rrs, NonBlockingSession session) {
        super(rrs, session);
        if (rrs.getNodes() == null) {
            throw new IllegalArgumentException("routeNode is null!");
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("execute multi node query " + rrs.getStatement());
        }
    }


    public void execute() throws Exception {
        lock.lock();
        try {
            this.reset();
            this.fieldsReturned = false;
        } finally {
            lock.unlock();
        }
        LOGGER.debug("rrs.getRunOnSlave()-" + rrs.getRunOnSlave());
        StringBuilder sb = new StringBuilder();
        for (final RouteResultsetNode node : rrs.getNodes()) {
            unResponseRrns.add(node);
            if (node.isModifySQL()) {
                sb.append("[").append(node.getName()).append("]").append(node.getStatement()).append(";\n");
            }
        }
        if (sb.length() > 0) {
            TxnLogHelper.putTxnLog(session.getSource(), sb.toString());
        }

        DDLTraceManager.getInstance().updateDDLStatus(DDLTraceInfo.DDLStage.EXECUTE_START, session.getSource());
        for (final RouteResultsetNode node : rrs.getNodes()) {
            BackendConnection conn = session.getTarget(node);
            if (session.tryExistsCon(conn, node)) {
                node.setRunOnSlave(rrs.getRunOnSlave());
                innerExecute(conn, node);
            } else {
                connRrns.add(node);
                node.setRunOnSlave(rrs.getRunOnSlave());
                PhysicalDataNode dn = DbleServer.getInstance().getConfig().getDataNodes().get(node.getName());
                dn.getConnection(dn.getDatabase(), session.getSource().isTxStart(), sessionAutocommit, node, this, node);
            }
        }
    }


    @Override
    public void errorResponse(byte[] data, BackendConnection conn) {
        DDLTraceManager.getInstance().updateConnectionStatus(session.getSource(), (MySQLConnection) conn,
                DDLTraceInfo.DDLConnectionStatus.CONN_EXECUTE_ERROR);
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.read(data);
        byte lastPacketId = packetId;
        errPacket.setPacketId(++lastPacketId); //just for normal error
        err = errPacket;
        session.resetMultiStatementStatus();
        lock.lock();
        try {
            if (!isFail()) {
                setFail(new String(err.getMessage()));
            }
            if (errConnection == null) {
                errConnection = new ArrayList<>();
            }
            errConnection.add(conn);
            if (decrementToZero(conn)) {
                session.handleSpecial(rrs, false, getDDLErrorInfo());
                DDLTraceManager.getInstance().endDDL(session.getSource(), getDDLErrorInfo());
                packetId++;
                if (byteBuffer != null) {
                    session.getSource().write(byteBuffer);
                }
                handleEndPacket(errPacket.toBytes(), false);
            }
        } finally {
            lock.unlock();
        }
    }


    @Override
    public void connectionClose(BackendConnection conn, String reason) {
        DDLTraceManager.getInstance().updateConnectionStatus(session.getSource(), (MySQLConnection) conn,
                DDLTraceInfo.DDLConnectionStatus.EXECUTE_CONN_CLOSE);
        if (checkClosedConn(conn)) {
            return;
        }
        LOGGER.warn("backend connect " + reason + ", conn info:" + conn);
        ErrorPacket errPacket = new ErrorPacket();
        byte lastPacketId = packetId;
        errPacket.setPacketId(++lastPacketId);
        errPacket.setErrNo(ErrorCode.ER_ABORTING_CONNECTION);
        reason = "Connection {DataHost[" + conn.getHost() + ":" + conn.getPort() + "],Schema[" + conn.getSchema() + "],threadID[" +
                ((MySQLConnection) conn).getThreadId() + "]} was closed ,reason is [" + reason + "]";
        errPacket.setMessage(StringUtil.encode(reason, session.getSource().getCharset().getResults()));
        err = errPacket;
        session.resetMultiStatementStatus();
        lock.lock();
        try {
            RouteResultsetNode rNode = (RouteResultsetNode) conn.getAttachment();
            unResponseRrns.remove(rNode);
            session.getTargetMap().remove(rNode);
            conn.setResponseHandler(null);
            executeError(conn);
        } finally {
            lock.unlock();
        }
    }


    private void innerExecute(BackendConnection conn, RouteResultsetNode node) {
        //do ddl what ever the serverConnection is closed
        MySQLConnection mysqlCon = (MySQLConnection) conn;
        mysqlCon.setResponseHandler(this);
        mysqlCon.setSession(session);
        DDLTraceManager.getInstance().updateConnectionStatus(session.getSource(), mysqlCon,
                DDLTraceInfo.DDLConnectionStatus.CONN_EXECUTE_START);
        mysqlCon.executeMultiNode(node, session.getSource(), sessionAutocommit && !session.getSource().isTxStart());
    }


    @Override
    public void okResponse(byte[] data, BackendConnection conn) {
        boolean executeResponse = conn.syncAndExecute();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("received ok response ,executeResponse:" + executeResponse + " from " + conn);
        }
        if (executeResponse) {
            DDLTraceManager.getInstance().updateConnectionStatus(session.getSource(), (MySQLConnection) conn,
                    DDLTraceInfo.DDLConnectionStatus.CONN_EXECUTE_SUCCESS);
            session.setBackendResponseEndTime((MySQLConnection) conn);
            ServerConnection source = session.getSource();
            OkPacket ok = new OkPacket();
            ok.read(data);
            lock.lock();
            try {
                if (!decrementToZero(conn))
                    return;
                if (isFail()) {
                    DDLTraceManager.getInstance().endDDL(source, "ddl end with execution failure");
                    session.handleSpecial(rrs, false);
                    session.resetMultiStatementStatus();
                    handleEndPacket(err.toBytes(), false);
                } else {
                    DDLTraceManager.getInstance().updateDDLStatus(DDLTraceInfo.DDLStage.META_UPDATE, source);
                    boolean metaInited = session.handleSpecial(rrs, true);
                    if (!metaInited) {
                        DDLTraceManager.getInstance().endDDL(source, "ddl end with meta failure");
                        executeMetaDataFailed();
                    } else {
                        DDLTraceManager.getInstance().endDDL(source, null);
                        ok.setPacketId(++packetId); // OK_PACKET
                        ok.setMessage(null);
                        ok.setAffectedRows(0);
                        ok.setServerStatus(source.isAutocommit() ? 2 : 1);
                        boolean multiStatementFlag = session.multiStatementPacket(ok, packetId);
                        doSqlStat();
                        handleEndPacket(ok.toBytes(), true);
                        session.multiStatementNextSql(multiStatementFlag);
                    }
                }
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public void connectionError(Throwable e, BackendConnection conn) {
        DDLTraceManager.getInstance().updateConnectionStatus(session.getSource(), (MySQLConnection) conn, DDLTraceInfo.DDLConnectionStatus.EXECUTE_CONN_ERROR);
        super.connectionError(e, conn);
    }

    private void executeMetaDataFailed() {
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.setPacketId(++packetId);
        errPacket.setErrNo(ErrorCode.ER_META_DATA);
        String errMsg = "Create TABLE OK, but generate metedata failed. The reason may be that the current druid parser can not recognize part of the sql" +
                " or the user for backend mysql does not have permission to execute the heartbeat sql.";
        errPacket.setMessage(StringUtil.encode(errMsg, session.getSource().getCharset().getResults()));
        session.multiStatementPacket(errPacket, packetId);
        doSqlStat();
        handleEndPacket(errPacket.toBytes(), false);
    }


    private boolean checkClosedConn(BackendConnection conn) {
        lock.lock();
        try {
            if (closedConnSet == null) {
                closedConnSet = new HashSet<>(1);
                closedConnSet.add(conn);
            } else if (closedConnSet.contains(conn)) {
                return true;
            } else {
                closedConnSet.add(conn);
            }
            this.getSession().getTargetMap().remove(conn.getAttachment());
            return false;
        } finally {
            lock.unlock();
        }
    }


    private void executeError(BackendConnection conn) {
        if (!isFail()) {
            setFail(new String(err.getMessage()));
        }
        if (errConnection == null) {
            errConnection = new ArrayList<>();
        }
        errConnection.add(conn);
        if (canResponse()) {
            session.handleSpecial(rrs, false);
            DDLTraceManager.getInstance().endDDL(session.getSource(), new String(err.getMessage()));
            packetId++;
            if (byteBuffer == null) {
                handleEndPacket(err.toBytes(), false);
            } else {
                session.getSource().write(byteBuffer);
                handleEndPacket(err.toBytes(), false);
            }
        }
    }

    private String getDDLErrorInfo() {
        StringBuilder s = new StringBuilder();
        s.append("{");
        for (int i = 0; i < errConnection.size(); i++) {
            BackendConnection conn = errConnection.get(i);
            s.append("\n ").append(FormatUtil.format(i + 1, 3));
            s.append(" -> ").append(conn.compactInfo());
        }
        s.append("\n}");

        return s.toString();
    }


    private void handleEndPacket(byte[] data, boolean isSuccess) {
        session.clearResources(false);
        session.setResponseTime(isSuccess);
        session.getSource().write(data);
    }

}
