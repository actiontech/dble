/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese
 * opensource volunteers. you can redistribute it and/or modify it under the
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Any questions about this component can be directed to it's project Web address
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.backend.mysql.nio.handler;

import io.mycat.MycatServer;
import io.mycat.backend.BackendConnection;
import io.mycat.backend.datasource.PhysicalDBNode;
import io.mycat.backend.mysql.nio.handler.transaction.AutoTxOperation;
import io.mycat.config.ErrorCode;
import io.mycat.config.MycatConfig;
import io.mycat.log.transaction.TxnLogHelper;
import io.mycat.net.mysql.ErrorPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.route.RouteResultset;
import io.mycat.route.RouteResultsetNode;
import io.mycat.route.util.RouteResultCopy;
import io.mycat.server.NonBlockingSession;
import io.mycat.server.ServerConnection;
import io.mycat.server.parser.ServerParse;
import io.mycat.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author guoji.ma@gmail.com
 */
public class MultiNodeDdlHandler extends MultiNodeHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiNodeDdlHandler.class);

    private static final String STMT = "select 1";
    private final RouteResultset rrs;
    private final RouteResultset orirrs;
    private final NonBlockingSession session;
    private final boolean sessionAutocommit;
    private final MultiNodeQueryHandler handler;

    private final ReentrantLock lock;

    protected volatile boolean terminated;

    private ErrorPacket err;
    private List<BackendConnection> errConnection;

    public MultiNodeDdlHandler(int sqlType, RouteResultset rrs, NonBlockingSession session) {
        super(session);
        if (rrs.getNodes() == null) {
            throw new IllegalArgumentException("routeNode is null!");
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("execute mutinode query " + rrs.getStatement());
        }

        this.rrs = RouteResultCopy.rrCopy(rrs, ServerParse.SELECT, STMT);
        this.sessionAutocommit = session.getSource().isAutocommit();
        this.session = session;

        this.orirrs = rrs;
        this.handler = new MultiNodeQueryHandler(sqlType, rrs, session);

        this.lock = new ReentrantLock();
    }

    protected void reset(int initCount) {
        super.reset(initCount);
        this.terminated = false;
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

        MycatConfig conf = MycatServer.getInstance().getConfig();
        LOGGER.debug("rrs.getRunOnSlave()-" + rrs.getRunOnSlave());
        StringBuilder sb = new StringBuilder();
        for (final RouteResultsetNode node : rrs.getNodes()) {
            if (node.isModifySQL()) {
                sb.append("[" + node.getName() + "]" + node.getStatement()).append(";\n");
            }
        }
        if (sb.length() > 0) {
            TxnLogHelper.putTxnLog(session.getSource(), sb.toString());
        }

        for (final RouteResultsetNode node : rrs.getNodes()) {
            BackendConnection conn = session.getTarget(node);
            if (session.tryExistsCon(conn, node)) {
                LOGGER.debug("node.getRunOnSlave()-" + node.getRunOnSlave());
                node.setRunOnSlave(rrs.getRunOnSlave());    // 实现 master/slave注解
                LOGGER.debug("node.getRunOnSlave()-" + node.getRunOnSlave());
                innerExecute(conn, node);
            } else {
                // create new connection
                LOGGER.debug("node.getRunOnSlave()1-" + node.getRunOnSlave());
                node.setRunOnSlave(rrs.getRunOnSlave());    // 实现 master/slave注解
                LOGGER.debug("node.getRunOnSlave()2-" + node.getRunOnSlave());
                PhysicalDBNode dn = conf.getDataNodes().get(node.getName());
                dn.getConnection(dn.getDatabase(), sessionAutocommit, node, this, node);
            }
        }
    }

    private void innerExecute(BackendConnection conn, RouteResultsetNode node) {
        if (clearIfSessionClosed(session)) {
            return;
        }
        conn.setResponseHandler(this);
        conn.execute(node, session.getSource(), sessionAutocommit && !session.getSource().isTxstart());
    }

    @Override
    public void connectionClose(BackendConnection conn, String reason) {
        LOGGER.warn("backend connect" + reason);
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.setPacketId(++packetId);
        errPacket.setErrno(ErrorCode.ER_ABORTING_CONNECTION);
        errPacket.setMessage(StringUtil.encode(reason, session.getSource().getCharset()));
        err = errPacket;

        lock.lock();
        try {
            if (!terminated) {
                terminated = true;
            }
            if (errConnection == null) {
                errConnection = new ArrayList<>();
            }
            errConnection.add(conn);
            if (!conn.syncAndExcute()) {
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
        LOGGER.warn("backend connect", e);
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.setPacketId(++packetId);
        errPacket.setErrno(ErrorCode.ER_ABORTING_CONNECTION);
        errPacket.setMessage(StringUtil.encode(e.toString(), session.getSource().getCharset()));
        err = errPacket;

        lock.lock();
        try {
            if (!terminated) {
                terminated = true;
            }
            if (errConnection == null) {
                errConnection = new ArrayList<>();
            }
            errConnection.add(conn);
            if (!conn.syncAndExcute()) {
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
            if (!isFail())
                setFail(err.toString());
            if (!conn.syncAndExcute()) {
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
        if (!conn.syncAndExcute()) {
            LOGGER.debug("MultiNodeDdlHandler should not arrive here(okResponse) !");
        }
    }

    @Override
    public void rowEofResponse(final byte[] eof, boolean isLeft, BackendConnection conn) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("on row end reseponse " + conn);
        }

        if (errorRepsponsed.get()) {
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

            if (errConnection == null) {
                try {
                    if (session.isPrepared()) {
                        handler.setPrepared(true);
                    }
                    handler.execute();
                } catch (Exception e) {
                    LOGGER.warn(String.valueOf(source) + orirrs, e);
                    source.writeErrMessage(ErrorCode.ERR_HANDLE_DATA, e.toString());
                }
                if (session.isPrepared()) {
                    session.setPrepared(false);
                }
            } else {
                handleEndPacket(err.toBytes(), AutoTxOperation.ROLLBACK, conn);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPacketsnull, byte[] eof,
                                 boolean isLeft, BackendConnection conn) {
    }

    @Override
    public boolean rowResponse(final byte[] row, RowDataPacket rowPacketnull, boolean isLeft, BackendConnection conn) {
        /* It is impossible arriving here, because we set limit to 0 */
        return errorRepsponsed.get();
    }

    @Override
    public void clearResources() {
    }

    @Override
    public void writeQueueAvailable() {
    }


    protected void handleEndPacket(byte[] data, AutoTxOperation txOperation, BackendConnection conn) {
        ServerConnection source = session.getSource();
        boolean inTransaction = !source.isAutocommit() || source.isTxstart();
        if (!inTransaction) {
            // 普通查询
            session.releaseConnection(conn);
        }
        // 显示分布式事务
        if (inTransaction && (AutoTxOperation.ROLLBACK == txOperation)) {
            source.setTxInterrupt("ROLLBACK");
        }
        if (nodeCount == 0) {
            session.getSource().write(data);
        }
    }
}
