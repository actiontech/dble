/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.impl.manager;

import com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.BaseDMLHandler;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.util.HandlerTool;
import com.oceanbase.obsharding_d.net.Session;
import com.oceanbase.obsharding_d.net.connection.FrontendConnection;
import com.oceanbase.obsharding_d.net.mysql.*;
import com.oceanbase.obsharding_d.net.service.AbstractService;
import com.oceanbase.obsharding_d.net.service.ResultFlag;
import com.oceanbase.obsharding_d.net.service.WriteFlags;
import com.oceanbase.obsharding_d.services.manager.ManagerSession;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/*
 * send back to client handler
 */
public class ManagerOutputHandler extends BaseDMLHandler {
    private static Logger logger = LoggerFactory.getLogger(ManagerOutputHandler.class);
    protected final ReentrantLock lock;

    private byte packetId = 0;
    private ByteBuffer buffer;
    private final ManagerSession managerSession;

    public ManagerOutputHandler(long id, Session session) {
        super(id, session);
        managerSession = (ManagerSession) session;
        this.lock = new ReentrantLock();
        this.buffer = managerSession.getSource().allocate();
    }

    @Override
    public HandlerType type() {
        return HandlerType.FINAL;
    }

    @Override
    public void okResponse(byte[] ok, @NotNull AbstractService service) {
    }

    @Override
    public void errorResponse(byte[] err, @NotNull AbstractService service) {
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.read(err);
        logger.info(service.toString() + "|errorResponse()|" + new String(errPacket.getMessage()));
        lock.lock();
        try {
            buffer = managerSession.getSource().getService().writeToBuffer(err, buffer);
            managerSession.getSource().getService().writeDirectly(buffer, WriteFlags.SESSION_END, ResultFlag.ERROR);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void fieldEofResponse(byte[] headerNull, List<byte[]> fieldsNull, List<FieldPacket> fieldPackets,
                                 byte[] eofNull, boolean isLeft, @NotNull AbstractService service) {
        managerSession.setHandlerStart(this);
        if (terminate.get()) {
            return;
        }
        lock.lock();
        try {
            if (terminate.get()) {
                return;
            }
            ResultSetHeaderPacket hp = new ResultSetHeaderPacket();
            hp.setFieldCount(fieldPackets.size());
            hp.setPacketId(++packetId);
            FrontendConnection source = managerSession.getSource();
            buffer = hp.write(buffer, source.getService(), true);
            for (FieldPacket fp : fieldPackets) {
                fp.setPacketId(++packetId);
                buffer = fp.write(buffer, source.getService(), true);
            }
            EOFPacket ep = new EOFPacket();
            ep.setPacketId(++packetId);
            buffer = ep.write(buffer, source.getService(), true);
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
            byte[] row;
            if (rowPacket != null) {
                rowPacket.setPacketId(++packetId);
                buffer = rowPacket.write(buffer, managerSession.getSource().getService(), true);
            } else {
                row = rowNull;
                row[3] = ++packetId;
                buffer = managerSession.getSource().getService().writeToBuffer(row, buffer);
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
        FrontendConnection source = managerSession.getSource();
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
            HandlerTool.terminateHandlerTree(this);
            byte[] eof = eofPacket.toBytes();
            buffer = source.getService().writeToBuffer(eof, buffer);
            managerSession.setHandlerEnd(this);
            source.getService().writeDirectly(buffer, WriteFlags.QUERY_END, ResultFlag.EOF_ROW);
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected void onTerminate() {
    }

}
