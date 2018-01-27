/*
* Copyright (C) 2016-2018 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.datasource.PhysicalDBNode;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.AutoTxOperation;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.log.alarm.AlarmCode;
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
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author guoji.ma@gmail.com
 */
public class MultiNodeDdlHandler extends MultiNodeHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiNodeDdlHandler.class);

    private static final String STMT = "select 1";
    private final RouteResultset rrs;
    private final RouteResultset oriRrs;
    private final NonBlockingSession session;
    private final boolean sessionAutocommit;
    private final MultiNodeQueryHandler handler;
    private ErrorPacket err;
    private volatile boolean errConn = false;
    private Set<BackendConnection> closedConnSet;

    public MultiNodeDdlHandler(RouteResultset rrs, NonBlockingSession session) {
        super(session);
        if (rrs.getNodes() == null) {
            throw new IllegalArgumentException("routeNode is null!");
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("execute multiNode query " + rrs.getStatement());
        }

        this.rrs = RouteResultCopy.rrCopy(rrs, ServerParse.SELECT, STMT);
        this.sessionAutocommit = session.getSource().isAutocommit();
        this.session = session;

        this.oriRrs = rrs;
        this.handler = new MultiNodeQueryHandler(rrs, session);

        this.errConn = false;
    }

    protected void reset(int initCount) {
        super.reset(initCount);
        this.errConn = false;
    }

    public NonBlockingSession getSession() {
        return session;
    }

    public void execute() throws Exception {
        lock.lock();
        try {
            this.reset(rrs.getNodes().length);
        } finally {
            lock.unlock();
        }

        LOGGER.debug("rrs.getRunOnSlave()-" + rrs.getRunOnSlave());
        StringBuilder sb = new StringBuilder();
        for (final RouteResultsetNode node : rrs.getNodes()) {
            if (node.isModifySQL()) {
                sb.append("[").append(node.getName()).append("]").append(node.getStatement()).append(";\n");
            }
        }
        if (sb.length() > 0) {
            TxnLogHelper.putTxnLog(session.getSource(), sb.toString());
        }

        for (final RouteResultsetNode node : rrs.getNodes()) {
            BackendConnection conn = session.getTarget(node);
            if (session.tryExistsCon(conn, node)) {
                node.setRunOnSlave(rrs.getRunOnSlave());
                innerExecute(conn, node);
            } else {
                // create new connection
                node.setRunOnSlave(rrs.getRunOnSlave());
                PhysicalDBNode dn = DbleServer.getInstance().getConfig().getDataNodes().get(node.getName());
                dn.getConnection(dn.getDatabase(), true, sessionAutocommit, node, this, node);
            }
        }
    }

    private void innerExecute(BackendConnection conn, RouteResultsetNode node) {
        if (clearIfSessionClosed(session)) {
            return;
        }
        conn.setResponseHandler(this);
        conn.setSession(session);
        ((MySQLConnection) conn).setComplexQuery(true);
        conn.execute(node, session.getSource(), sessionAutocommit && !session.getSource().isTxStart());
    }

    @Override
    public void connectionClose(BackendConnection conn, String reason) {
        if (checkClosedConn(conn)) {
            return;
        }
        LOGGER.info("backend connect" + reason);
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.setPacketId(++packetId);
        errPacket.setErrNo(ErrorCode.ER_ABORTING_CONNECTION);
        errPacket.setMessage(StringUtil.encode(reason, session.getSource().getCharset().getResults()));
        err = errPacket;

        executeConnError(conn);
    }

    private boolean checkClosedConn(BackendConnection conn) {
        lock.lock();
        try {
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

    private void executeConnError(BackendConnection conn) {
        lock.lock();
        try {
            if (!isFail()) {
                setFail(new String(err.getMessage()));
            }
            errConn = true;
            if (!conn.syncAndExecute()) {
                return;
            }
            if (--nodeCount <= 0) {
                handleEndPacket(err.toBytes(), AutoTxOperation.ROLLBACK, conn);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void connectionError(Throwable e, BackendConnection conn) {
        LOGGER.info("backend connect", e);
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.setPacketId(++packetId);
        errPacket.setErrNo(ErrorCode.ER_ABORTING_CONNECTION);
        errPacket.setMessage(StringUtil.encode(e.toString(), session.getSource().getCharset().getResults()));
        err = errPacket;

        executeConnError(conn);
    }

    @Override
    public void connectionAcquired(final BackendConnection conn) {
        final RouteResultsetNode node = (RouteResultsetNode) conn.getAttachment();
        session.bindConnection(node, conn);
        innerExecute(conn, node);
    }


    @Override
    public void errorResponse(byte[] data, BackendConnection conn) {
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.read(data);
        errPacket.setPacketId(1);
        err = errPacket;
        lock.lock();
        try {
            if (!isFail()) {
                setFail(new String(errPacket.getMessage()));
            }
            if (!conn.syncAndExecute()) {
                return;
            }
            if (--nodeCount > 0)
                return;
            handleEndPacket(err.toBytes(), AutoTxOperation.ROLLBACK, conn);
        } finally {
            lock.unlock();
        }
    }

    /* arriving here is impossible */
    @Override
    public void okResponse(byte[] data, BackendConnection conn) {
        if (!conn.syncAndExecute()) {
            LOGGER.debug("MultiNodeDdlHandler should not arrive here(okResponse) !");
        }
    }

    @Override
    public void rowEofResponse(final byte[] eof, boolean isLeft, BackendConnection conn) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("on row end response " + conn);
        }

        if (errorResponse.get()) {
            return;
        }

        final ServerConnection source = session.getSource();
        if (clearIfSessionClosed(session)) {
            return;
        }

        lock.lock();
        try {
            if (this.isFail() || session.closed()) {
                tryErrorFinished(true);
                return;
            }

            if (--nodeCount > 0)
                return;

            if (errConn) {
                handleEndPacket(err.toBytes(), AutoTxOperation.ROLLBACK, conn);
            } else {
                try {
                    if (session.isPrepared()) {
                        handler.setPrepared(true);
                    }
                    handler.execute();
                } catch (Exception e) {
                    LOGGER.warn(AlarmCode.CORE_DDL_WARN + String.valueOf(source) + oriRrs, e);
                    session.handleSpecial(oriRrs, source.getSchema(), false);
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
        return errorResponse.get();
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


    private void handleEndPacket(byte[] data, AutoTxOperation txOperation, BackendConnection conn) {
        ServerConnection source = session.getSource();
        boolean inTransaction = !source.isAutocommit() || source.isTxStart();
        if (!inTransaction) {
            // normal query
            session.releaseConnection(conn);
        }
        // Explicit distributed transaction
        if (inTransaction && (AutoTxOperation.ROLLBACK == txOperation)) {
            source.setTxInterrupt("ROLLBACK");
        }
        if (nodeCount == 0) {
            session.getSource().write(data);
        }
    }
}
