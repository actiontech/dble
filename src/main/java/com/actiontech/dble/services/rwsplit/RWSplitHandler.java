package com.actiontech.dble.services.rwsplit;

import com.actiontech.dble.backend.mysql.nio.handler.LoadDataResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.PreparedResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.util.StringUtil;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;

public class RWSplitHandler implements ResponseHandler, LoadDataResponseHandler, PreparedResponseHandler, StatisticsHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(RWSplitHandler.class);
    private final RWSplitService rwSplitService;
    private final byte[] originPacket;
    private final AbstractConnection frontedConnection;
    protected volatile ByteBuffer buffer;
    /**
     * When client send one request. dble should return one and only one response.
     * But , maybe OK event and connection closed event are run in parallel.
     * so we need use synchronized and write2Client to prevent conflict.
     */
    private boolean write2Client = false;
    private final Callback callback;
    private final boolean isHint;
    /**
     * If there are more packets next.This flag in would be set.
     */
    private static final int HAS_MORE_RESULTS = 0x08;

    public RWSplitHandler(RWSplitService service, byte[] originPacket, Callback callback, boolean isHint) {
        this.rwSplitService = service;
        this.originPacket = originPacket;
        this.frontedConnection = service.getConnection();
        this.callback = callback;
        this.isHint = isHint;
    }

    public void execute(final BackendConnection conn) {
        MySQLResponseService mysqlService = conn.getBackendService();
        mysqlService.setResponseHandler(this);
        if (originPacket != null) {
            mysqlService.execute(rwSplitService, originPacket);
        } else if (isHint) {
            //remove comment sentences
            mysqlService.execute(rwSplitService, rwSplitService.getExecuteSql());
        } else {
            //ensure that the character set is consistent with the client
            mysqlService.execute(rwSplitService, rwSplitService.getExecuteSqlBytes());
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
                if (buffer != null) {
                    buffer = rwSplitService.writeToBuffer(data, buffer);
                    frontedConnection.write(buffer);
                } else {
                    frontedConnection.write(data);
                }
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

            final OkPacket packet = new OkPacket();
            packet.read(data);
            if ((packet.getServerStatus() & HAS_MORE_RESULTS) == 0) {
                if (callback != null) {
                    callback.callback(true, rwSplitService);
                }
                rwSplitService.getSession().unbindIfSafe();
            }

            synchronized (this) {
                if (!write2Client) {
                    data[3] = (byte) rwSplitService.nextPacketId();
                    frontedConnection.write(data);
                    if ((packet.getServerStatus() & HAS_MORE_RESULTS) == 0) {
                        write2Client = true;
                    }
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
                if ((eof[7] & HAS_MORE_RESULTS) == 0) {
                    /*
                    last resultset will call this
                     */
                    rwSplitService.getSession().unbindIfSafe();
                } else {
                    LOGGER.debug("Because of multi query had send.It would receive more than one ResultSet. recycle resource should be delayed. client:{}", service);
                }
                buffer = frontedConnection.writeToBuffer(eof, buffer);
                /*
                multi statement all cases are as follows:
                1. if an resultSet is followed by an resultSet. buffer will re-assign in fieldEofResponse()
                2. if an resultSet is followed by an okResponse. okResponse() send directly without use buffer.
                3. if an resultSet is followed by  an errorResponse. buffer will be used if it is not null.
                We must prevent  same buffer called connection.write() twice.
                According to the above, you need write buffer immediately and set buffer to null.
                 */
                frontedConnection.write(buffer);
                buffer = null;
                if ((eof[7] & HAS_MORE_RESULTS) == 0) {
                    write2Client = true;
                }
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
                reason = "Connection {dbInstance[" + service.getConnection().getHost() + ":" + service.getConnection().getPort() + "],DbGroup[" +
                        rwSplitService.getUserConfig().getDbGroup() + "],threadID[" +
                        ((MySQLResponseService) service).getConnection().getThreadId() + "]} was closed ,reason is [" + reason + "]";
                writeErrorMsg(rwSplitService.nextPacketId(), reason);
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

    @Override
    public void stringEof(byte[] data, @NotNull AbstractService service) {
        synchronized (this) {
            if (!write2Client) {
                if (buffer == null) {
                    buffer = frontedConnection.allocate();
                }
                buffer = frontedConnection.getService().writeToBuffer(data, buffer);
                frontedConnection.write(buffer);
                write2Client = true;
                buffer = null;
            }
        }
    }
}
