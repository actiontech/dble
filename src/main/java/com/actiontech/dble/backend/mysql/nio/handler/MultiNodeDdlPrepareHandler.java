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
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.route.util.RouteResultCopy;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.singleton.DDLTraceManager;
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author guoji.ma@gmail.com
 */
public class MultiNodeDdlPrepareHandler extends MultiNodeHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiNodeDdlPrepareHandler.class);

    private static final String STMT = "select 1";
    private final RouteResultset rrs;
    private final RouteResultset oriRrs;
    private final boolean sessionAutocommit;
    private final MultiNodeDDLExecuteHandler handler;
    private ErrorPacket err;
    private Set<BackendConnection> closedConnSet;
    private volatile boolean finishedTest = false;
    private AtomicBoolean releaseDDLLock = new AtomicBoolean(false);

    public MultiNodeDdlPrepareHandler(RouteResultset rrs, NonBlockingSession session) {
        super(session);
        if (rrs.getNodes() == null) {
            throw new IllegalArgumentException("routeNode is null!");
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("execute multiNode query " + rrs.getStatement());
        }

        this.rrs = RouteResultCopy.rrCopy(rrs, ServerParse.DDL, STMT);
        this.sessionAutocommit = session.getSource().isAutocommit();

        this.oriRrs = rrs;
        this.handler = new MultiNodeDDLExecuteHandler(rrs, session);
    }

    @Override
    protected void reset() {
        super.reset();
    }

    public NonBlockingSession getSession() {
        return session;
    }

    public void execute() throws Exception {
        lock.lock();
        try {
            this.reset();
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

        DDLTraceManager.getInstance().updateDDLStatus(DDLTraceInfo.DDLStage.CONN_TEST_START, session.getSource());

        for (final RouteResultsetNode node : rrs.getNodes()) {
            BackendConnection conn = session.getTarget(node);
            if (session.tryExistsCon(conn, node)) {
                node.setRunOnSlave(rrs.getRunOnSlave());
                innerExecute(conn, node);
            } else {
                // create new connection
                node.setRunOnSlave(rrs.getRunOnSlave());
                PhysicalDataNode dn = DbleServer.getInstance().getConfig().getDataNodes().get(node.getName());
                dn.getConnection(dn.getDatabase(), true, sessionAutocommit, node, this, node);
            }
        }
    }

    private void innerExecute(BackendConnection conn, RouteResultsetNode node) {
        if (clearIfSessionClosed()) {
            return;
        }
        conn.setResponseHandler(this);
        conn.setSession(session);
        ((MySQLConnection) conn).setTesting(true);
        ((MySQLConnection) conn).setComplexQuery(true);
        conn.execute(node, session.getSource(), sessionAutocommit && !session.getSource().isTxStart());
    }

    @Override
    public void connectionClose(BackendConnection conn, String reason) {
        DDLTraceManager.getInstance().updateConnectionStatus(session.getSource(),
                (MySQLConnection) conn, DDLTraceInfo.DDLConnectionStatus.TEST_CONN_CLOSE);
        if (checkClosedConn(conn)) {
            return;
        }
        LOGGER.info("backend connect" + reason);
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.setPacketId(1);
        errPacket.setErrNo(ErrorCode.ER_ABORTING_CONNECTION);
        errPacket.setMessage(StringUtil.encode(reason, session.getSource().getCharset().getResults()));
        err = errPacket;

        lock.lock();
        try {
            RouteResultsetNode rNode = (RouteResultsetNode) conn.getAttachment();
            unResponseRrns.remove(rNode);
            executeConnError();
        } finally {
            lock.unlock();
        }
    }

    private boolean checkClosedConn(BackendConnection conn) {
        lock.lock();
        try {
            if (finishedTest) {
                return true;
            }
            if (closedConnSet == null) {
                closedConnSet = new HashSet<>(1);
                closedConnSet.add(conn);
            } else {
                if (closedConnSet.contains(conn)) {
                    return true;
                }
                closedConnSet.add(conn);
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    private void executeConnError() {
        if (!isFail()) {
            setFail(new String(err.getMessage()));
        }
        if (canResponse() && errorResponse.compareAndSet(false, true)) {
            if (releaseDDLLock.compareAndSet(false, true)) {
                session.handleSpecial(oriRrs, false);
            }
            handleRollbackPacket(err.toBytes(), "DDL prepared failed");
        }
    }

    @Override
    public void connectionError(Throwable e, BackendConnection conn) {
        LOGGER.info("backend connect", e);
        DDLTraceManager.getInstance().updateConnectionStatus(session.getSource(),
                (MySQLConnection) conn, DDLTraceInfo.DDLConnectionStatus.TEST_CONN_ERROR);
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.setPacketId(1);
        errPacket.setErrNo(ErrorCode.ER_ABORTING_CONNECTION);
        errPacket.setMessage(StringUtil.encode(e.toString(), session.getSource().getCharset().getResults()));
        err = errPacket;

        lock.lock();
        try {
            errorConnsCnt++;
            executeConnError();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void connectionAcquired(final BackendConnection conn) {
        final RouteResultsetNode node = (RouteResultsetNode) conn.getAttachment();
        session.bindConnection(node, conn);
        DDLTraceManager.getInstance().updateConnectionStatus(session.getSource(),
                (MySQLConnection) conn, DDLTraceInfo.DDLConnectionStatus.CONN_TEST_START);
        innerExecute(conn, node);
    }


    @Override
    public void errorResponse(byte[] data, BackendConnection conn) {
        DDLTraceManager.getInstance().updateConnectionStatus(session.getSource(),
                (MySQLConnection) conn, DDLTraceInfo.DDLConnectionStatus.CONN_TEST_RESULT_ERROR);
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.read(data);
        errPacket.setPacketId(1);
        err = errPacket;
        LOGGER.info("ddl do select 1 errorResponse:", err.getMessage());
        lock.lock();
        try {
            if (!isFail()) {
                setFail(new String(errPacket.getMessage()));
            }
            if (decrementToZero(conn) && errorResponse.compareAndSet(false, true)) {
                session.handleSpecial(oriRrs, false);
                handleRollbackPacket(err.toBytes(), "DDL prepared failed");
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void okResponse(byte[] data, BackendConnection conn) {
        if (!conn.syncAndExecute()) {
            LOGGER.debug("MultiNodeDdlPrepareHandler syncAndExecute!");
        } else {
            LOGGER.debug("MultiNodeDdlPrepareHandler syncAndExecute finished!");
        }
    }

    @Override
    public void rowEofResponse(final byte[] eof, boolean isLeft, BackendConnection conn) {
        DDLTraceManager.getInstance().updateConnectionStatus(session.getSource(),
                (MySQLConnection) conn, DDLTraceInfo.DDLConnectionStatus.CONN_TEST_SUCCESS);
        final ServerConnection source = session.getSource();
        if (clearIfSessionClosed()) {
            return;
        }

        lock.lock();
        try {
            ((MySQLConnection) conn).setTesting(false);
            if (!decrementToZero(conn))
                return;

            if (this.isFail()) {
                if (errorResponse.compareAndSet(false, true)) {
                    session.handleSpecial(oriRrs, false);
                    handleRollbackPacket(err.toBytes(), "DDL prepared failed");
                }
            } else {
                try {
                    DDLTraceManager.getInstance().updateDDLStatus(DDLTraceInfo.DDLStage.CONN_TEST_END, source);
                    if (session.isPrepared()) {
                        handler.setPrepared(true);
                    }
                    finishedTest = true;
                    session.setTraceSimpleHandler(handler);
                    session.setPreExecuteEnd(false);
                    if (!session.isKilled()) {
                        handler.execute();
                    } else {
                        DDLTraceManager.getInstance().endDDL(source, "Query was interrupted");
                        session.handleSpecial(oriRrs, false);
                        ErrorPacket errPacket = new ErrorPacket();
                        errPacket.setPacketId(++packetId);
                        errPacket.setErrNo(ErrorCode.ER_QUERY_INTERRUPTED);
                        errPacket.setMessage(StringUtil.encode("Query was interrupted", session.getSource().getCharset().getResults()));
                        handleRollbackPacket(errPacket.toBytes(), "Query was interrupted");
                    }
                } catch (Exception e) {
                    DDLTraceManager.getInstance().endDDL(source, "take Connection error:" + e.getMessage());
                    LOGGER.warn(String.valueOf(source) + oriRrs, e);
                    session.handleSpecial(oriRrs, false);
                    source.writeErrMessage(ErrorCode.ERR_HANDLE_DATA, e.toString());
                }
                if (session.isPrepared()) {
                    session.setPrepared(false);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPacketsNull, byte[] eof,
                                 boolean isLeft, BackendConnection conn) {
    }

    @Override
    public boolean rowResponse(final byte[] row, RowDataPacket rowPacketNull, boolean isLeft, BackendConnection conn) {
        /* It is impossible arriving here, because we set limit to 0 */
        return false;
    }

    @Override
    public void clearResources() {
        if (closedConnSet != null) {
            closedConnSet.clear();
        }
    }

    @Override
    public void writeQueueAvailable() {
    }


    private void handleRollbackPacket(byte[] data, String reason) {
        ServerConnection source = session.getSource();
        boolean inTransaction = !source.isAutocommit() || source.isTxStart();
        if (!inTransaction) {
            // normal query
            session.closeAndClearResources(reason);
        } else {
            // Explicit distributed transaction
            source.setTxInterrupt(reason);
        }
        session.getSource().write(data);
    }


    public boolean clearIfSessionClosed() {
        if (session.closed()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("session closed without execution,clear resources " + session);
            }
            session.clearResources(true);
            if (releaseDDLLock.compareAndSet(false, true)) {
                session.handleSpecial(oriRrs, false);
            }
            this.clearResources();
            return true;
        } else {
            return false;
        }
    }
}
