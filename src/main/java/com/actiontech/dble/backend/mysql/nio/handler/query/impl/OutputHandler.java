/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.query.impl;

import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.nio.handler.query.BaseDMLHandler;
import com.actiontech.dble.backend.mysql.nio.handler.util.HandlerTool;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.net.FrontendConnection;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.net.Session;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.statistic.stat.QueryResult;
import com.actiontech.dble.statistic.stat.QueryResultDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/*
 * send back to client handler
 */
public class OutputHandler extends BaseDMLHandler {
    private static Logger logger = LoggerFactory.getLogger(OutputHandler.class);
    protected final ReentrantLock lock;

    private byte packetId;
    private ByteBuffer buffer;
    private boolean isBinary;
    private long netOutBytes;
    private long selectRows;
    private final NonBlockingSession serverSession;

    public OutputHandler(long id, Session session) {
        super(id, session);
        serverSession = (NonBlockingSession) session;
        serverSession.setOutputHandler(this);
        this.lock = new ReentrantLock();
        this.packetId = (byte) serverSession.getPacketId().get();
        this.isBinary = serverSession.isPrepared();
        this.buffer = serverSession.getSource().allocate();
    }

    @Override
    public HandlerType type() {
        return HandlerType.FINAL;
    }

    @Override
    public void okResponse(byte[] ok, BackendConnection conn) {
        this.netOutBytes += ok.length;
        OkPacket okPacket = new OkPacket();
        okPacket.read(ok);
        FrontendConnection source = serverSession.getSource();
        lock.lock();
        try {
            ok[3] = ++packetId;
            serverSession.multiStatementPacket(okPacket, packetId);
            boolean multiStatementFlag = serverSession.getIsMultiStatement().get();
            if ((okPacket.getServerStatus() & StatusFlags.SERVER_MORE_RESULTS_EXISTS) > 0) {
                buffer = source.writeToBuffer(ok, buffer);
            } else {
                HandlerTool.terminateHandlerTree(this);
                buffer = source.writeToBuffer(ok, buffer);
                source.write(buffer);
            }
            serverSession.multiStatementNextSql(multiStatementFlag);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void errorResponse(byte[] err, BackendConnection conn) {
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.read(err);
        logger.info(conn.toString() + "|errorResponse()|" + new String(errPacket.getMessage()));
        lock.lock();
        try {
            buffer = serverSession.getSource().writeToBuffer(err, buffer);
            serverSession.resetMultiStatementStatus();
            serverSession.getSource().write(buffer);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void fieldEofResponse(byte[] headerNull, List<byte[]> fieldsNull, List<FieldPacket> fieldPackets,
                                 byte[] eofNull, boolean isLeft, BackendConnection conn) {
        serverSession.setHandlerStart(this);
        if (terminate.get()) {
            return;
        }
        lock.lock();
        try {
            if (terminate.get()) {
                return;
            }
            if (this.isBinary)
                this.fieldPackets = fieldPackets;
            ResultSetHeaderPacket hp = new ResultSetHeaderPacket();
            hp.setFieldCount(fieldPackets.size());
            hp.setPacketId(++packetId);
            this.netOutBytes += hp.calcPacketSize();
            FrontendConnection source = serverSession.getSource();
            buffer = hp.write(buffer, source, true);
            for (FieldPacket fp : fieldPackets) {
                fp.setPacketId(++packetId);
                this.netOutBytes += fp.calcPacketSize();
                buffer = fp.write(buffer, source, true);
            }
            EOFPacket ep = new EOFPacket();
            ep.setPacketId(++packetId);
            this.netOutBytes += ep.calcPacketSize();
            buffer = ep.write(buffer, source, true);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean rowResponse(byte[] rowNull, RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        if (terminate.get()) {
            return true;
        }
        lock.lock();
        try {
            if (terminate.get()) {
                return true;
            }
            selectRows++;
            byte[] row;
            if (this.isBinary) {
                BinaryRowDataPacket binRowPacket = new BinaryRowDataPacket();
                binRowPacket.read(this.fieldPackets, rowPacket);
                binRowPacket.setPacketId(++packetId);
                this.netOutBytes += binRowPacket.calcPacketSize();
                buffer = binRowPacket.write(buffer, serverSession.getSource(), true);
                this.packetId = (byte) serverSession.getPacketId().get();
            } else {
                if (rowPacket != null) {
                    rowPacket.setPacketId(++packetId);
                    this.netOutBytes += rowPacket.calcPacketSize();
                    buffer = rowPacket.write(buffer, serverSession.getSource(), true);
                    this.packetId = (byte) serverSession.getPacketId().get();
                } else {
                    row = rowNull;
                    this.netOutBytes += row.length;
                    boolean isBigPackage = row.length >= MySQLPacket.MAX_PACKET_SIZE + MySQLPacket.PACKET_HEADER_SIZE;
                    if (isBigPackage) {
                        buffer = serverSession.getSource().writeBigPackageToBuffer(row, buffer, packetId);
                        this.packetId = (byte) serverSession.getPacketId().get();
                    } else {
                        row[3] = ++packetId;
                        buffer = serverSession.getSource().writeToBuffer(row, buffer);
                    }
                }
            }
        } finally {
            lock.unlock();
        }
        return false;
    }

    @Override
    public void rowEofResponse(byte[] data, boolean isLeft, BackendConnection conn) {
        if (terminate.get()) {
            return;
        }
        logger.debug("--------sql execute end!");
        FrontendConnection source = serverSession.getSource();
        lock.lock();
        try {
            if (terminate.get()) {
                return;
            }
            EOFPacket eofPacket = new EOFPacket();
            if (data != null) {
                eofPacket.read(data);
            }
            eofPacket.setPacketId(++packetId);
            this.netOutBytes += eofPacket.calcPacketSize();
            doSqlStat();
            HandlerTool.terminateHandlerTree(this);
            serverSession.multiStatementPacket(eofPacket, packetId);
            byte[] eof = eofPacket.toBytes();
            buffer = source.writeToBuffer(eof, buffer);
            serverSession.setHandlerEnd(this);
            serverSession.setResponseTime(true);
            boolean multiStatementFlag = serverSession.getIsMultiStatement().get();
            source.write(buffer);
            serverSession.multiStatementNextSql(multiStatementFlag);
        } finally {
            lock.unlock();
        }
    }

    private void doSqlStat() {
        if (SystemConfig.getInstance().getUseSqlStat() == 1) {
            long netInBytes = 0;
            String sql = serverSession.getSource().getExecuteSql();
            if (sql != null) {
                netInBytes += sql.getBytes().length;
                QueryResult queryResult = new QueryResult(serverSession.getSource().getUser(), ServerParse.SELECT,
                        sql, selectRows, netInBytes, netOutBytes, serverSession.getQueryStartTime(), System.currentTimeMillis(), netOutBytes);
                if (logger.isDebugEnabled()) {
                    logger.debug("try to record sql:" + sql);
                }
                QueryResultDispatcher.dispatchQuery(queryResult);
            }
        }
    }

    public void backendConnError(byte[] errMsg) {
        if (terminate.compareAndSet(false, true)) {
            serverSession.resetMultiStatementStatus();
            ErrorPacket err = new ErrorPacket();
            err.setErrNo(ErrorCode.ER_YES);
            err.setMessage(errMsg);
            HandlerTool.terminateHandlerTree(this);
            backendConnError(err);
        }
    }

    private void backendConnError(ErrorPacket error) {
        lock.lock();
        try {
            recycleResources();
            if (error == null) {
                error = new ErrorPacket();
                error.setErrNo(ErrorCode.ER_YES);
                error.setMessage("unknown error".getBytes());
            }
            error.setPacketId(++packetId);
            serverSession.getSource().write(error.toBytes());
        } finally {
            lock.unlock();
        }
    }

    private void recycleResources() {
        if (buffer != null) {
            if (buffer.position() > 0) {
                serverSession.getSource().write(buffer);
            } else {
                serverSession.getSource().recycle(buffer);
                buffer = null;
            }
        }
        serverSession.resetMultiStatementStatus();
    }

    @Override
    protected void onTerminate() {
        if (this.isBinary) {
            if (this.fieldPackets != null)
                this.fieldPackets.clear();
        }
    }

}
