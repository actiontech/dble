/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.query.impl;

import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.nio.handler.query.BaseDMLHandler;
import com.actiontech.dble.backend.mysql.nio.handler.util.HandlerTool;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.ServerConnection;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/*
 * send back to client handler
 */
public class OutputHandler extends BaseDMLHandler {
    private static Logger logger = Logger.getLogger(OutputHandler.class);
    protected final ReentrantLock lock;

    private byte packetId;
    private NonBlockingSession session;
    private ByteBuffer buffer;
    private boolean isBinary;
    private boolean hasNext;

    public OutputHandler(long id, NonBlockingSession session, boolean hasNext) {
        super(id, session);
        session.setOutputHandler(this);
        this.lock = new ReentrantLock();
        this.packetId = 0;
        this.session = session;
        this.hasNext = hasNext;
        this.isBinary = session.isPrepared();
        this.buffer = session.getSource().allocate();
    }

    @Override
    public HandlerType type() {
        return HandlerType.FINAL;
    }

    @Override
    public void okResponse(byte[] ok, BackendConnection conn) {
        OkPacket okPacket = new OkPacket();
        okPacket.read(ok);
        ServerConnection source = session.getSource();
        lock.lock();
        try {
            ok[3] = ++packetId;
            if ((okPacket.getServerStatus() & StatusFlags.SERVER_MORE_RESULTS_EXISTS) > 0) {
                buffer = source.writeToBuffer(ok, buffer);
            } else {
                HandlerTool.terminateHandlerTree(this);
                if (hasNext) {
                    okPacket.setServerStatus(okPacket.getServerStatus() | StatusFlags.SERVER_MORE_RESULTS_EXISTS);
                }
                buffer = source.writeToBuffer(ok, buffer);
                if (hasNext) {
                    source.write(buffer);
                    //source.excuteNext(packetId, false);
                } else {
                    //source.excuteNext(packetId, false);
                    source.write(buffer);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void errorResponse(byte[] err, BackendConnection conn) {
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.read(err);
        logger.warn(new StringBuilder().append(conn.toString()).append("|errorResponse()|").append(new String(errPacket.getMessage())));
        lock.lock();
        try {
            buffer = session.getSource().writeToBuffer(err, buffer);
            //session.getSource().excuteNext(packetId, true);
            session.getSource().write(buffer);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void fieldEofResponse(byte[] headernull, List<byte[]> fieldsnull, List<FieldPacket> fieldPackets,
                                 byte[] eofnull, boolean isLeft, BackendConnection conn) {
        if (terminate.get()) {
            return;
        }
        lock.lock();
        try {
            if (this.isBinary)
                this.fieldPackets = fieldPackets;
            ResultSetHeaderPacket hp = new ResultSetHeaderPacket();
            hp.setFieldCount(fieldPackets.size());
            hp.setPacketId(++packetId);

            ServerConnection source = session.getSource();
            buffer = hp.write(buffer, source, true);
            for (FieldPacket fp : fieldPackets) {
                fp.setPacketId(++packetId);
                buffer = fp.write(buffer, source, true);
            }
            EOFPacket ep = new EOFPacket();
            ep.setPacketId(++packetId);
            buffer = ep.write(buffer, source, true);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean rowResponse(byte[] rownull, RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        if (terminate.get()) {
            return true;
        }
        lock.lock();
        try {
            byte[] row;
            if (this.isBinary) {
                BinaryRowDataPacket binRowPacket = new BinaryRowDataPacket();
                binRowPacket.read(this.fieldPackets, rowPacket);
                binRowPacket.setPacketId(++packetId);
                buffer = binRowPacket.write(buffer, session.getSource(), true);
            } else {
                if (rowPacket != null) {
                    rowPacket.setPacketId(++packetId);
                    buffer = rowPacket.write(buffer, session.getSource(), true);
                } else {
                    row = rownull;
                    row[3] = ++packetId;
                    buffer = session.getSource().writeToBuffer(row, buffer);
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
        logger.info("--------sql execute end!");
        ServerConnection source = session.getSource();
        lock.lock();
        try {
            EOFPacket eofPacket = new EOFPacket();
            if (data != null) {
                eofPacket.read(data);
            }
            eofPacket.setPacketId(++packetId);
            if (hasNext) {
                eofPacket.setStatus(eofPacket.getStatus() | StatusFlags.SERVER_MORE_RESULTS_EXISTS);
            }
            HandlerTool.terminateHandlerTree(this);
            byte[] eof = eofPacket.toBytes();
            buffer = source.writeToBuffer(eof, buffer);
            if (hasNext) {
                source.write(buffer);
                //source.excuteNext(packetId, false);
            } else {
                //source.excuteNext(packetId, false);
                source.write(buffer);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void relayPacketResponse(byte[] relayPacket, BackendConnection conn) {
        lock.lock();
        try {
            buffer = session.getSource().writeToBuffer(relayPacket, buffer);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void endPacketResponse(byte[] endPacket, BackendConnection conn) {
        lock.lock();
        try {
            buffer = session.getSource().writeToBuffer(endPacket, buffer);
            session.getSource().write(buffer);
        } finally {
            lock.unlock();
        }
    }

    public void backendConnError(byte[] errMsg) {
        if (terminate.compareAndSet(false, true)) {
            ErrorPacket err = new ErrorPacket();
            err.setErrno(ErrorCode.ER_YES);
            err.setMessage(errMsg);
            HandlerTool.terminateHandlerTree(this);
            backendConnError(err);
        }
    }

    protected void backendConnError(ErrorPacket error) {
        lock.lock();
        try {
            recycleResources();
            if (error == null) {
                error = new ErrorPacket();
                error.setErrno(ErrorCode.ER_YES);
                error.setMessage("unknown error".getBytes());
            }
            error.setPacketId(++packetId);
            //session.getSource().excuteNext(packetId, true);
            session.getSource().write(error.toBytes());
        } finally {
            lock.unlock();
        }
    }

    private void recycleResources() {
        if (buffer != null) {
            if (buffer.position() > 0) {
                session.getSource().write(buffer);
            } else {
                session.getSource().recycle(buffer);
                buffer = null;
            }
        }
    }

    @Override
    protected void onTerminate() {
        if (this.isBinary) {
            if (this.fieldPackets != null)
                this.fieldPackets.clear();
        }
    }

}
