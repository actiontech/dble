/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.rwsplit;

import com.actiontech.dble.backend.mysql.nio.handler.LoadDataResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.PreparedResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.log.sqldump.SqlDumpLogHelper;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.connection.FrontendConnection;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.net.service.ResultFlag;
import com.actiontech.dble.net.service.WriteFlags;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.util.StringUtil;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class RWSplitHandler implements ResponseHandler, LoadDataResponseHandler, PreparedResponseHandler, ShowFieldsHandler, StatisticsHandler {

    protected static final Logger LOGGER = LoggerFactory.getLogger(RWSplitHandler.class);
    protected final RWSplitService rwSplitService;
    protected final AbstractConnection frontedConnection;

    protected volatile ByteBuffer buffer;
    protected long selectRows = 0;
    protected long netOutBytes;
    protected long resultSize;
    /**
     * When client send one request. dble should return one and only one response.
     * But , maybe OK event and connection closed event are run in parallel.
     * so we need use synchronized and write2Client to prevent conflict.
     */
    protected boolean write2Client = false;

    protected boolean isUseOriginPacket;
    protected final byte[] originPacket;
    protected volatile String executeSql;
    protected volatile Callback callback;

    public RWSplitHandler(RWSplitService service, boolean isUseOriginPacket, byte[] originPacket, Callback callback) {
        this.rwSplitService = service;
        this.frontedConnection = service.getConnection();
        this.isUseOriginPacket = isUseOriginPacket;
        this.originPacket = originPacket;
        this.executeSql = rwSplitService.getExecuteSql();
        this.callback = callback;
    }

    public void execute(final BackendConnection conn) {
        MySQLResponseService mysqlService = conn.getBackendService();
        mysqlService.setResponseHandler(this);
        mysqlService.setSession(rwSplitService.getSession2());
        if (isUseOriginPacket) {
            // ensure that the character set is consistent with the client
            mysqlService.execute(rwSplitService, originPacket);
        } else if (!StringUtil.isEmpty(executeSql)) {
            // such as: Hint sql (remove comment sentences)
            mysqlService.execute(rwSplitService, executeSql, rwSplitService.isForceUseAutoCommit());
        } else {
            // not happen
            mysqlService.execute(rwSplitService, rwSplitService.getExecuteSqlBytes());
        }
    }

    @Override
    public void connectionAcquired(final BackendConnection conn) {
        if (null != rwSplitService.getSession2().getRwGroup()) {
            rwSplitService.getSession2().getRwGroup().unBindRwSplitSession(rwSplitService.getSession2());
        }
        rwSplitService.getSession2().bind(conn);
        execute(conn);
    }

    @Override
    public void connectionError(Throwable e, Object attachment) {
        if (null != rwSplitService.getSession2().getRwGroup()) {
            rwSplitService.getSession2().getRwGroup().unBindRwSplitSession(rwSplitService.getSession2());
        }
        loadDataClean();
        initDbClean();
        writeErrorMsg(rwSplitService.nextPacketId(), "can't connect to dbGroup[" + rwSplitService.getUserConfig().getDbGroup());
    }

    @Override
    public void errorResponse(byte[] data, @NotNull AbstractService service) {
        MySQLResponseService mysqlService = (MySQLResponseService) service;
        ErrorPacket errPg = new ErrorPacket();
        errPg.read(data);
        final boolean syncFinished = mysqlService.syncAndExecute();
        loadDataClean();
        initDbClean();
        if (callback != null && errPg.getErrNo() != ErrorCode.ER_PARSE_ERROR) { // 1064, ignore
            callback.callback(false, null, rwSplitService);
        }
        rwSplitService.getSession2().recordLastSqlResponseTime();
        if (!syncFinished) {
            mysqlService.getConnection().businessClose("unfinished sync");
            rwSplitService.getSession2().unbind();
        } else {
            rwSplitService.getSession2().unbindIfSafe();
        }
        synchronized (this) {
            if (!write2Client) {
                SqlDumpLogHelper.info(executeSql, originPacket, rwSplitService, mysqlService, 0);
                data[3] = (byte) rwSplitService.nextPacketId();
                if (buffer != null) {
                    buffer = rwSplitService.writeToBuffer(data, buffer);
                    frontedConnection.getService().writeDirectly(buffer, WriteFlags.SESSION_END, ResultFlag.ERROR);
                } else {
                    rwSplitService.write(data, WriteFlags.SESSION_END);
                }
                write2Client = true;
            }
        }
    }

    @Override
    public void okResponse(byte[] data, @NotNull AbstractService service) {
        MySQLResponseService mysqlService = (MySQLResponseService) service;
        this.netOutBytes += data.length;
        boolean executeResponse = mysqlService.syncAndExecute();
        if (executeResponse) {

            this.resultSize += data.length;
            final OkPacket packet = new OkPacket();
            packet.read(data);
            loadDataClean();
            synchronized (this) {
                if (!write2Client) {
                    rwSplitService.getSession2().recordLastSqlResponseTime();
                    if (callback != null)
                        callback.callback(true, null, rwSplitService);
                    rwSplitService.getSession2().trace(t -> t.setBackendResponseEndTime(mysqlService));
                    SqlDumpLogHelper.info(executeSql, originPacket, rwSplitService, mysqlService, packet.getAffectedRows());
                    data[3] = (byte) rwSplitService.nextPacketId();
                    rwSplitService.getSession2().unbindIfSafe();
                    rwSplitService.getSession2().trace(t -> t.doSqlStat(packet.getAffectedRows(), netOutBytes, resultSize));
                    rwSplitService.write(data, WriteFlags.QUERY_END, ResultFlag.OK);
                    write2Client = true;
                }
            }
        }
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPacketsNull, byte[] eof,
                                 boolean isLeft, @NotNull AbstractService service) {
        buffer = frontedConnection.allocate();
        synchronized (this) {
            this.netOutBytes += header.length;
            this.resultSize += header.length;
            for (byte[] field : fields) {
                this.netOutBytes += field.length;
                this.resultSize += field.length;
            }
            this.netOutBytes += eof.length;
            this.resultSize += eof.length;
            header[3] = (byte) rwSplitService.nextPacketId();
            buffer = frontedConnection.getService().writeToBuffer(header, buffer);
            for (byte[] field : fields) {
                field[3] = (byte) rwSplitService.nextPacketId();
                buffer = frontedConnection.getService().writeToBuffer(field, buffer);
            }
            eof[3] = (byte) rwSplitService.nextPacketId();
            buffer = frontedConnection.getService().writeToBuffer(eof, buffer);
        }
    }

    @Override
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, @NotNull AbstractService service) {
        synchronized (this) {
            this.netOutBytes += row.length;
            this.resultSize += row.length;
            selectRows++;
            if (rwSplitService.isInitDb()) {
                rwSplitService.getTableRows().set(selectRows);
            }
            if (buffer == null) {
                buffer = frontedConnection.allocate();
            }
            row[3] = (byte) rwSplitService.nextPacketId();
            buffer = frontedConnection.getService().writeToBuffer(row, buffer);
        }
        return false;
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, @NotNull AbstractService service) {
        synchronized (this) {
            if (!write2Client) {
                this.netOutBytes += eof.length;
                this.resultSize += eof.length;
                rwSplitService.getSession2().recordLastSqlResponseTime();
                if (callback != null)
                    callback.callback(true, null, rwSplitService);
                SqlDumpLogHelper.info(executeSql, originPacket, rwSplitService, (MySQLResponseService) service, selectRows);
                eof[3] = (byte) rwSplitService.nextPacketId();
                rwSplitService.getSession2().unbindIfSafe();
                rwSplitService.getSession2().trace(t -> t.doSqlStat(selectRows, netOutBytes, resultSize));
                buffer = frontedConnection.getService().writeToBuffer(eof, buffer);
                frontedConnection.getService().writeDirectly(buffer, WriteFlags.QUERY_END, ResultFlag.EOF_ROW);
                buffer = null;
                selectRows = 0;
                write2Client = true;
            }
        }
    }

    @Override
    public void requestDataResponse(byte[] requestFilePacket, @Nonnull MySQLResponseService service) {
        synchronized (this) {
            if (!write2Client) {
                rwSplitService.write(requestFilePacket, WriteFlags.QUERY_END);
            }
        }
    }


    @Override
    public void connectionClose(@NotNull AbstractService service, String reason) {
        ((MySQLResponseService) service).setResponseHandler(null);
        synchronized (this) {
            if (!write2Client) {
                loadDataClean();
                initDbClean();
                rwSplitService.getSession2().unbind();
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
    public void preparedOkResponse(byte[] ok, List<byte[]> fields, List<byte[]> params, MySQLResponseService service) {
        synchronized (this) {
            if (buffer == null) {
                buffer = frontedConnection.allocate();
            }
            if (!write2Client) {
                ok[3] = (byte) rwSplitService.nextPacketId();
                buffer = frontedConnection.getService().writeToBuffer(ok, buffer);
                if (params != null) {
                    for (byte[] param : params) {
                        param[3] = (byte) rwSplitService.nextPacketId();
                        buffer = frontedConnection.getService().writeToBuffer(param, buffer);
                    }
                }
                if (fields != null) {
                    for (byte[] field : fields) {
                        field[3] = (byte) rwSplitService.nextPacketId();
                        buffer = frontedConnection.getService().writeToBuffer(field, buffer);
                    }
                }
                if (callback != null) {
                    callback.callback(true, ok, rwSplitService);
                }
                SqlDumpLogHelper.info(executeSql, originPacket, rwSplitService, service, 0);
                frontedConnection.getService().writeDirectly(buffer, WriteFlags.QUERY_END);
                write2Client = true;
                buffer = null;
            }
        }
    }

    @Override
    public void preparedExecuteResponse(byte[] header, List<byte[]> fields, byte[] eof, MySQLResponseService service) {
        synchronized (this) {
            if (buffer == null) {
                buffer = frontedConnection.allocate();
            }
            if (!write2Client) {
                header[3] = (byte) rwSplitService.nextPacketId();
                buffer = frontedConnection.getService().writeToBuffer(header, buffer);
                if (fields != null) {
                    for (byte[] field : fields) {
                        field[3] = (byte) rwSplitService.nextPacketId();
                        buffer = frontedConnection.getService().writeToBuffer(field, buffer);
                    }
                }
                eof[3] = (byte) rwSplitService.nextPacketId();
                buffer = frontedConnection.getService().writeToBuffer(eof, buffer);
                frontedConnection.getService().writeDirectly(buffer, WriteFlags.QUERY_END);
                SqlDumpLogHelper.info(executeSql, originPacket, rwSplitService, service, 0);
                write2Client = true;
                buffer = null;
            }
        }
    }

    protected void writeErrorMsg(int pId, String reason) {
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.setPacketId(pId);
        errPacket.setErrNo(ErrorCode.ER_DB_INSTANCE_ABORTING_CONNECTION);
        errPacket.setMessage(StringUtil.encode(reason, rwSplitService.getCharset().getClient()));
        errPacket.write(frontedConnection);
    }

    protected void loadDataClean() {
        if (rwSplitService.isInLoadData()) {
            FrontendConnection connection = (FrontendConnection) rwSplitService.getConnection();
            connection.setSkipCheck(false);
        }
    }

    protected void initDbClean() {
        if (rwSplitService.isInitDb()) {
            rwSplitService.setInitDb(false);
            rwSplitService.setTableRows(new AtomicLong());
        }
    }

    @Override
    public void fieldsEof(byte[] header, List<byte[]> fields, byte[] eof, @Nonnull AbstractService service) {
        synchronized (this) {
            if (!write2Client) {
                buffer = frontedConnection.allocate();
                header[3] = (byte) rwSplitService.nextPacketId();
                buffer = frontedConnection.getService().writeToBuffer(header, buffer);
                for (byte[] field : fields) {
                    field[3] = (byte) rwSplitService.nextPacketId();
                    buffer = frontedConnection.getService().writeToBuffer(field, buffer);
                }
                eof[3] = (byte) rwSplitService.nextPacketId();
                if (rwSplitService.isInitDb()) {
                    if (rwSplitService.getTableRows().decrementAndGet() == 0) {
                        initDbClean();
                        rwSplitService.getSession2().unbindIfSafe();
                    }
                }
                buffer = frontedConnection.getService().writeToBuffer(eof, buffer);
                frontedConnection.getService().writeDirectly(buffer, WriteFlags.QUERY_END);
                write2Client = true;
                buffer = null;
            }
        }
    }

    @Override
    public void stringEof(byte[] data, @NotNull AbstractService service) {
        synchronized (this) {
            if (!write2Client) {
                if (buffer == null) {
                    buffer = frontedConnection.allocate();
                }
                buffer = frontedConnection.getService().writeToBuffer(data, buffer);
                frontedConnection.getService().writeDirectly(buffer, WriteFlags.QUERY_END);
                write2Client = true;
                buffer = null;
            }
        }
    }
}
