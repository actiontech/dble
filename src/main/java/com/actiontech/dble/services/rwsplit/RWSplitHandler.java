package com.actiontech.dble.services.rwsplit;

import com.actiontech.dble.backend.mysql.nio.handler.LoadDataResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.PreparedResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.List;

public class RWSplitHandler implements ResponseHandler, LoadDataResponseHandler, PreparedResponseHandler {

    private final RWSplitService rwSplitService;
    private final byte[] originPacket;
    private final AbstractConnection frontedConnection;
    protected volatile ByteBuffer buffer;
    private boolean write2Client = false;
    private final Callback callback;

    public RWSplitHandler(RWSplitService service, byte[] originPacket, Callback callback) {
        this.rwSplitService = service;
        this.originPacket = originPacket;
        this.frontedConnection = service.getConnection();
        this.callback = callback;
    }

    public void execute(final BackendConnection conn) {
        MySQLResponseService mysqlService = conn.getBackendService();
        mysqlService.setResponseHandler(this);
        if (originPacket != null) {
            mysqlService.execute(rwSplitService, originPacket);
        } else {
            mysqlService.execute(rwSplitService, rwSplitService.getExecuteSql());
        }
    }

    @Override
    public void connectionAcquired(final BackendConnection conn) {
        rwSplitService.getSession().bind(conn);
        execute(conn);
    }

    @Override
    public void connectionError(Throwable e, Object attachment) {
        writeErrorMsg(rwSplitService.nextPacketId(), "can't connect to dbGroup[" + rwSplitService.getUserConfig().getDbGroup());
    }

    @Override
    public void errorResponse(byte[] data, AbstractService service) {
        MySQLResponseService mysqlService = (MySQLResponseService) service;
        boolean syncFinished = mysqlService.syncAndExecute();
        if (callback != null) {
            callback.callback(false, rwSplitService);
        }
        if (!syncFinished) {
            mysqlService.getConnection().businessClose("unfinished sync");
            rwSplitService.getSession().unbind();
        } else {
            rwSplitService.getSession().unbindIfSafe();
        }
        synchronized (this) {
            if (!write2Client) {
                data[3] = (byte) rwSplitService.nextPacketId();
                frontedConnection.write(data);
                write2Client = true;
            }
        }
    }

    @Override
    public void okResponse(byte[] data, AbstractService service) {
        // TraceManager.TraceObject traceObject = TraceManager.serviceTrace(service, "get-ok-packet");
        //        TraceManager.finishSpan(service, traceObject);
        MySQLResponseService mysqlService = (MySQLResponseService) service;
        boolean executeResponse = mysqlService.syncAndExecute();
        if (executeResponse) {
            if (callback != null) {
                callback.callback(true, rwSplitService);
            }
            rwSplitService.getSession().unbindIfSafe();
            synchronized (this) {
                if (!write2Client) {
                    data[3] = (byte) rwSplitService.nextPacketId();
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
        synchronized (this) {
            header[3] = (byte) rwSplitService.nextPacketId();
            buffer = frontedConnection.writeToBuffer(header, buffer);
            for (byte[] field : fields) {
                field[3] = (byte) rwSplitService.nextPacketId();
                buffer = frontedConnection.writeToBuffer(field, buffer);
            }
            eof[3] = (byte) rwSplitService.nextPacketId();
            buffer = frontedConnection.writeToBuffer(eof, buffer);
        }
    }

    // this for prepared statement ok
    @Override
    public void fieldEofResponse(List<byte[]> fields, byte[] eof, MySQLResponseService service) {
        synchronized (this) {
            for (byte[] field : fields) {
                field[3] = (byte) rwSplitService.nextPacketId();
                buffer = frontedConnection.writeToBuffer(field, buffer);
            }
            eof[3] = (byte) rwSplitService.nextPacketId();
            buffer = frontedConnection.writeToBuffer(eof, buffer);
            frontedConnection.write(buffer);
        }
    }


    @Override
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, AbstractService service) {
        synchronized (this) {
            row[3] = (byte) rwSplitService.nextPacketId();
            buffer = frontedConnection.writeToBuffer(row, buffer);
        }
        return false;
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, AbstractService service) {
        synchronized (this) {
            if (!write2Client) {
                eof[3] = (byte) rwSplitService.nextPacketId();
                rwSplitService.getSession().unbindIfSafe();
                buffer = frontedConnection.writeToBuffer(eof, buffer);
                frontedConnection.write(buffer);
                write2Client = true;
            }
        }
    }

    @Override
    public void requestDataResponse(byte[] requestFilePacket, MySQLResponseService service) {
        synchronized (this) {
            if (!write2Client) {
                frontedConnection.write(requestFilePacket);
            }
        }
    }


    @Override
    public void connectionClose(AbstractService service, String reason) {
        ((MySQLResponseService) service).setResponseHandler(null);
        synchronized (this) {
            if (!write2Client) {
                rwSplitService.getSession().bind(null);
                writeErrorMsg(rwSplitService.nextPacketId(), "connection close");
                write2Client = true;
                if (buffer != null) {
                    frontedConnection.recycle(buffer);
                    buffer = null;
                }
            }
        }
    }

    @Override
    public void preparedOkResponse(byte[] ok, MySQLResponseService service) {
        boolean executeResponse = service.syncAndExecute();
        if (executeResponse) {
            synchronized (this) {
                if (buffer == null) {
                    buffer = frontedConnection.allocate();
                }
                if (!write2Client) {
                    ok[3] = (byte) rwSplitService.nextPacketId();
                    frontedConnection.write(ok);
                    write2Client = true;
                }
            }
        }
    }

    @Override
    public void paramEofResponse(List<byte[]> params, byte[] eof, MySQLResponseService service) {
        synchronized (this) {
            for (byte[] field : params) {
                field[3] = (byte) rwSplitService.nextPacketId();
                buffer = frontedConnection.writeToBuffer(field, buffer);
            }
            eof[3] = (byte) rwSplitService.nextPacketId();
            buffer = frontedConnection.writeToBuffer(eof, buffer);
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
