package com.actiontech.dble.services.rwsplit;

import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.singleton.TraceManager;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.List;

public class RWSplitHandler implements ResponseHandler {

    private final RWSplitService rwSplitService;
    private final AbstractConnection frontedConnection;
    protected volatile ByteBuffer buffer;
    private byte packetId = 0;
    private volatile int offset;
    private boolean write2Client = false;

    public RWSplitHandler(RWSplitService service) {
        this.rwSplitService = service;
        this.frontedConnection = service.getConnection();
    }

    @Override
    public void connectionAcquired(final BackendConnection conn) {
        conn.getBackendService().setResponseHandler(this);
        conn.getBackendService().execute(rwSplitService);
        offset = conn.getBackendService().getTotalSynCmdCount();
    }

    @Override
    public void connectionError(Throwable e, Object attachment) {
        writeErrorMsg(packetId++, "can't connect");
    }

    @Override
    public void errorResponse(byte[] data, AbstractService service) {
        boolean syncFinished = ((MySQLResponseService) service).syncAndExecute();
        if (!syncFinished) {
            service.getConnection().businessClose("unfinished sync");
        } else {
            ((MySQLResponseService) service).getConnection().release();
        }
        synchronized (this) {
            if (!write2Client) {
                data[3] -= offset;
                frontedConnection.write(data);
                write2Client = true;
            }
        }
    }

    @Override
    public void okResponse(byte[] data, AbstractService service) {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(service, "get-ok-packet");
        TraceManager.finishSpan(service, traceObject);
        boolean executeResponse = ((MySQLResponseService) service).syncAndExecute();
        if (executeResponse) {
            ((MySQLResponseService) service).getConnection().release();
            synchronized (this) {
                if (!write2Client) {
                    data[3] -= offset;
                    frontedConnection.write(data);
                    write2Client = true;
                }
            }
        }
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPacketsNull, byte[] eof,
                                 boolean isLeft, AbstractService service) {
        buffer = frontedConnection.allocate();
        header[3] -= offset;
        buffer = frontedConnection.writeToBuffer(header, buffer);
        for (byte[] field : fields) {
            field[3] -= offset;
            buffer = frontedConnection.writeToBuffer(field, buffer);
        }
        eof[3] -= offset;
        buffer = frontedConnection.writeToBuffer(eof, buffer);
        packetId = eof[3];
    }

    @Override
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, AbstractService service) {
        row[3] -= offset;
        buffer = frontedConnection.writeToBuffer(row, buffer);
        packetId = row[3];

        return false;
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, AbstractService service) {
        ((MySQLResponseService) service).getConnection().release();
        synchronized (this) {
            if (!write2Client) {
                packetId = 0;
                eof[3] -= offset;
                buffer = frontedConnection.writeToBuffer(eof, buffer);
                frontedConnection.write(buffer);
                write2Client = true;
            }
        }
    }

    @Override
    public void connectionClose(AbstractService service, String reason) {
        ((MySQLResponseService) service).setResponseHandler(null);
        synchronized (this) {
            if (!write2Client) {
                packetId = 0;
                writeErrorMsg(packetId++, "connection close");
                write2Client = true;
                if (buffer != null) {
                    frontedConnection.recycle(buffer);
                    buffer = null;
                }
            }
        }
    }

    private void writeErrorMsg(int pId, String reason) {
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.setPacketId(pId);
        errPacket.setErrNo(ErrorCode.ER_DB_INSTANCE_ABORTING_CONNECTION);
        errPacket.setMessage(StringUtil.encode(reason, frontedConnection.getCharsetName().getClient()));
        errPacket.write(frontedConnection);
    }

}
