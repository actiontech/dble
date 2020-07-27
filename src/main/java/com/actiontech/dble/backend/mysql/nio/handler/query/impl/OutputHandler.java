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

    public OutputHandler(long id, NonBlockingSession session) {
        super(id, session);
        session.setOutputHandler(this);
        this.lock = new ReentrantLock();
        this.isBinary = session.isPrepared();
        this.buffer = session.getFrontConnection().allocate();
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
        okPacket.setPacketId(session.getShardingService().nextPacketId());
        ShardingService sessionShardingService = session.getShardingService();
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
            buffer = session.getFrontConnection().writeToBuffer(err, buffer);
            session.resetMultiStatementStatus();
            session.getFrontConnection().write(buffer);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void fieldEofResponse(byte[] headerNull, List<byte[]> fieldsNull, List<FieldPacket> fieldPackets,
                                 byte[] eofNull, boolean isLeft, AbstractService service) {
        session.setHandlerStart(this);
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
            hp.setPacketId(session.getShardingService().nextPacketId());
            this.netOutBytes += hp.calcPacketSize();
            ShardingService shardingService = session.getShardingService();
            buffer = hp.write(buffer, shardingService, true);
            for (FieldPacket fp : fieldPackets) {
                fp.setPacketId(session.getShardingService().nextPacketId());
                this.netOutBytes += fp.calcPacketSize();
                buffer = fp.write(buffer, shardingService, true);
            }
            EOFPacket ep = new EOFPacket();
            ep.setPacketId(session.getShardingService().nextPacketId());
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
                binRowPacket.setPacketId(session.getShardingService().nextPacketId());
                this.netOutBytes += binRowPacket.calcPacketSize();
                buffer = binRowPacket.write(buffer, session.getShardingService(), true);
            } else {
                if (rowPacket != null) {
                    rowPacket.setPacketId(session.getShardingService().nextPacketId());
                    this.netOutBytes += rowPacket.calcPacketSize();
                    buffer = rowPacket.write(buffer, session.getShardingService(), true);
                } else {
                    row = rowNull;
                    RowDataPacket rowDataPk = new RowDataPacket(this.fieldPackets.size());
                    row[3] = (byte) session.getShardingService().nextPacketId();
                    rowDataPk.read(row);
                    this.netOutBytes += row.length;
                    rowDataPk.write(buffer, session.getShardingService(), true);
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
        ShardingService shardingService = session.getShardingService();
        lock.lock();
        try {
            if (terminate.get()) {
                return;
            }
            EOFRowPacket eofPacket = new EOFRowPacket();
            if (data != null) {
                eofPacket.read(data);
            }
            eofPacket.setPacketId(session.getShardingService().nextPacketId());
            this.netOutBytes += eofPacket.calcPacketSize();
            doSqlStat();
            HandlerTool.terminateHandlerTree(this);
            session.setHandlerEnd(this);
            session.setResponseTime(true);
            eofPacket.write(buffer, shardingService);
        } finally {
            lock.unlock();
        }
    }

    private void doSqlStat() {
        if (SystemConfig.getInstance().getUseSqlStat() == 1) {
            long netInBytes = 0;
            String sql = session.getShardingService().getExecuteSql();
            if (sql != null) {
                netInBytes += sql.getBytes().length;
                QueryResult queryResult = new QueryResult(session.getShardingService().getUser(), ServerParse.SELECT,
                        sql, selectRows, netInBytes, netOutBytes, session.getQueryStartTime(), System.currentTimeMillis(), netOutBytes);
                if (logger.isDebugEnabled()) {
                    logger.debug("try to record sql:" + sql);
                }
                QueryResultDispatcher.dispatchQuery(queryResult);
            }
        }
    }

    public void backendConnError(byte[] errMsg) {
        if (terminate.compareAndSet(false, true)) {
            session.resetMultiStatementStatus();
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
            error.setPacketId(session.getShardingService().nextPacketId());
            session.getFrontConnection().write(error.toBytes());
        } finally {
            lock.unlock();
        }
    }

    private void recycleResources() {
        if (buffer != null) {
            if (buffer.position() > 0) {
                session.getFrontConnection().write(buffer);
            } else {
                session.getFrontConnection().recycle(buffer);
                buffer = null;
            }
        }
        session.resetMultiStatementStatus();
    }

    @Override
    protected void onTerminate() {
        if (this.isBinary) {
            if (this.fieldPackets != null)
                this.fieldPackets.clear();
        }
    }

}
