/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.query.impl;

import com.actiontech.dble.backend.mysql.nio.handler.query.BaseDMLHandler;
import com.actiontech.dble.backend.mysql.nio.handler.util.HandlerTool;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.Session;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.net.service.ResultFlag;
import com.actiontech.dble.net.service.WriteFlags;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.RequestScope;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import static com.actiontech.dble.net.mysql.StatusFlags.SERVER_STATUS_CURSOR_EXISTS;

/*
 * send back to client handler
 */
public class OutputHandler extends BaseDMLHandler {
    private static Logger logger = LoggerFactory.getLogger(OutputHandler.class);
    protected final ReentrantLock lock;

    private volatile ByteBuffer buffer;
    private boolean isBinary;
    private long netOutBytes;
    private long selectRows;
    protected final NonBlockingSession serverSession;
    protected final RequestScope requestScope;

    public OutputHandler(long id, Session session) {
        super(id, session);
        serverSession = (NonBlockingSession) session;
        serverSession.setOutputHandler(this);
        this.lock = new ReentrantLock();
        this.requestScope = serverSession.getShardingService().getRequestScope();
        this.isBinary = requestScope.isPrepared();
    }

    @Override
    public HandlerType type() {
        return HandlerType.FINAL;
    }

    @Override
    public void okResponse(byte[] ok, @NotNull AbstractService service) {
        this.netOutBytes += ok.length;
        OkPacket okPacket = new OkPacket();
        okPacket.read(ok);
        okPacket.setPacketId(serverSession.getShardingService().nextPacketId());
        ShardingService sessionShardingService = serverSession.getShardingService();
        lock.lock();
        try {
            HandlerTool.terminateHandlerTree(this);
            okPacket.write(sessionShardingService.getConnection());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void errorResponse(byte[] err, @NotNull AbstractService service) {
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.read(err);
        logger.info(service.toString() + "|errorResponse()|" + new String(errPacket.getMessage()));
        lock.lock();
        try {
            serverSession.resetMultiStatementStatus();
            errPacket.write(serverSession.getShardingService().getConnection());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void fieldEofResponse(byte[] headerNull, List<byte[]> fieldsNull, List<FieldPacket> fieldPackets,
                                 byte[] eofNull, boolean isLeft, @NotNull AbstractService service) {
        serverSession.setHandlerStart(this);
        if (terminate.get()) {
            return;
        }
        if (requestScope.isUsingCursor()) {
            requestScope.getCurrentPreparedStatement().initCursor(serverSession, this, fieldPackets.size(), fieldPackets);
        }
        lock.lock();
        try {
            if (terminate.get()) {
                return;
            }

            if (buffer == null) {
                this.buffer = serverSession.getSource().allocate();
            }

            if (this.isBinary)
                this.fieldPackets = fieldPackets;
            ResultSetHeaderPacket hp = new ResultSetHeaderPacket();
            hp.setFieldCount(fieldPackets.size());
            hp.setPacketId(serverSession.getShardingService().nextPacketId());
            this.netOutBytes += hp.calcPacketSize();
            ShardingService shardingService = serverSession.getShardingService();
            buffer = hp.write(buffer, shardingService, true);
            for (FieldPacket fp : fieldPackets) {
                fp.setPacketId(serverSession.getShardingService().nextPacketId());
                this.netOutBytes += fp.calcPacketSize();
                buffer = fp.write(buffer, shardingService, true);
            }
            EOFPacket ep = new EOFPacket();
            ep.setPacketId(serverSession.getShardingService().nextPacketId());
            if (requestScope.isUsingCursor()) {
                byte statusFlag = 0;
                statusFlag |= serverSession.getShardingService().isAutocommit() ? 2 : 1;
                statusFlag |= SERVER_STATUS_CURSOR_EXISTS;
                ep.setStatus(statusFlag);
            }
            this.netOutBytes += ep.calcPacketSize();
            buffer = ep.write(buffer, shardingService, true);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean rowResponse(byte[] rowNull, RowDataPacket rowPacket, boolean isLeft, @NotNull AbstractService service) {
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
                if (requestScope.isUsingCursor()) {
                    requestScope.getCurrentPreparedStatement().getCursorCache().add(rowPacket);
                } else {
                    BinaryRowDataPacket binRowPacket = new BinaryRowDataPacket();
                    binRowPacket.read(this.fieldPackets, rowPacket);
                    binRowPacket.setPacketId(serverSession.getShardingService().nextPacketId());
                    this.netOutBytes += binRowPacket.calcPacketSize();
                    buffer = binRowPacket.write(buffer, serverSession.getShardingService(), true);
                }
            } else {
                if (rowPacket != null) {
                    rowPacket.setPacketId(serverSession.getShardingService().nextPacketId());
                    this.netOutBytes += rowPacket.calcPacketSize();
                    buffer = rowPacket.write(buffer, serverSession.getShardingService(), true);
                } else {
                    row = rowNull;
                    RowDataPacket rowDataPk = new RowDataPacket(this.fieldPackets.size());
                    row[3] = (byte) serverSession.getShardingService().nextPacketId();
                    rowDataPk.read(row);
                    this.netOutBytes += row.length;
                    buffer = rowDataPk.write(buffer, serverSession.getShardingService(), true);
                }
            }
        } finally {
            lock.unlock();
        }
        return false;
    }

    @Override
    public void rowEofResponse(byte[] data, boolean isLeft, @NotNull AbstractService service) {
        if (terminate.get()) {
            return;
        }
        logger.debug("--------sql execute end!");
        ShardingService shardingService = serverSession.getShardingService();
        if (requestScope.isUsingCursor()) {
            requestScope.getCurrentPreparedStatement().getCursorCache().done();
            HandlerTool.terminateHandlerTree(this);
            serverSession.setHandlerEnd(this);
            serverSession.getShardingService().writeDirectly(buffer, WriteFlags.QUERY_END, ResultFlag.EOF_ROW);
            return;
        }
        lock.lock();
        try {
            if (terminate.get()) {
                return;
            }
            EOFRowPacket eofPacket = new EOFRowPacket();
            if (data != null) {
                eofPacket.read(data);
            }
            eofPacket.setPacketId(serverSession.getShardingService().nextPacketId());
            this.netOutBytes += eofPacket.calcPacketSize();
            serverSession.trace(t -> t.doSqlStat(selectRows, netOutBytes, netOutBytes));
            HandlerTool.terminateHandlerTree(this);
            serverSession.setHandlerEnd(this);
            eofPacket.write(buffer, shardingService);
        } finally {
            lock.unlock();
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
            error.setPacketId(serverSession.getShardingService().nextPacketId());
            serverSession.getShardingService().write(error);
        } finally {
            lock.unlock();
        }
    }

    public void cleanBuffer() {
        if (buffer != null) {
            session.getSource().recycle(buffer);
        }
    }

    private void recycleResources() {
        if (buffer != null) {
            if (buffer.position() > 0) {
                serverSession.getShardingService().writeDirectly(buffer, WriteFlags.PART);
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

    @Override
    public ExplainType explainType() {
        return ExplainType.WRITE_TO_CLIENT;
    }

}
