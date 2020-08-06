/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.query.impl;

import com.actiontech.dble.backend.mysql.nio.handler.query.BaseDMLHandler;
import com.actiontech.dble.backend.mysql.nio.handler.util.HandlerTool;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.net.Session;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
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
        this.isBinary = serverSession.isPrepared();
        this.buffer = serverSession.getSource().allocate();
    }

    @Override
    public HandlerType type() {
        return HandlerType.FINAL;
    }

    @Override
    public void okResponse(byte[] ok, AbstractService service) {
        this.netOutBytes += ok.length;
        OkPacket okPacket = new OkPacket();
        okPacket.read(ok);
        okPacket.setPacketId(serverSession.getShardingService().nextPacketId());
        ShardingService sessionShardingService = serverSession.getShardingService();
        lock.lock();
        try {
            HandlerTool.terminateHandlerTree(this);
            okPacket.write(buffer, sessionShardingService);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void errorResponse(byte[] err, AbstractService service) {
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.read(err);
        logger.info(service.toString() + "|errorResponse()|" + new String(errPacket.getMessage()));
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
                                 byte[] eofNull, boolean isLeft, AbstractService service) {
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
            this.netOutBytes += ep.calcPacketSize();
            buffer = ep.write(buffer, shardingService, true);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean rowResponse(byte[] rowNull, RowDataPacket rowPacket, boolean isLeft, AbstractService service) {
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
                binRowPacket.setPacketId(serverSession.getShardingService().nextPacketId());
                this.netOutBytes += binRowPacket.calcPacketSize();
                buffer = binRowPacket.write(buffer, serverSession.getShardingService(), true);
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
    public void rowEofResponse(byte[] data, boolean isLeft, AbstractService service) {
        if (terminate.get()) {
            return;
        }
        logger.debug("--------sql execute end!");
        ShardingService shardingService = serverSession.getShardingService();
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
            doSqlStat();
            HandlerTool.terminateHandlerTree(this);
            serverSession.setHandlerEnd(this);
            serverSession.setResponseTime(true);
            eofPacket.write(buffer, shardingService);
        } finally {
            lock.unlock();
        }
    }

    private void doSqlStat() {
        if (SystemConfig.getInstance().getUseSqlStat() == 1) {
            long netInBytes = 0;
            String sql = serverSession.getShardingService().getExecuteSql();
            if (sql != null) {
                netInBytes += sql.getBytes().length;
                QueryResult queryResult = new QueryResult(serverSession.getShardingService().getUser(), ServerParse.SELECT,
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
            error.setPacketId(serverSession.getShardingService().nextPacketId());
            serverSession.getShardingService().write(error);
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
