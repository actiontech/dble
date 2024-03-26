package com.actiontech.dble.services.rwsplit;

import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.backend.mysql.ByteUtil;
import com.actiontech.dble.backend.mysql.nio.handler.PreparedResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.PreparedClosePacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.net.connection.AbstractConnection;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class PSHandler implements ResponseHandler, PreparedResponseHandler {

    private final RWSplitService rwSplitService;
    private final AbstractConnection frontedConnection;
    private final PreparedStatementHolder holder;
    private boolean write2Client = false;
    private long originStatementId;

    public PSHandler(RWSplitService service, PreparedStatementHolder holder) {
        this.rwSplitService = service;
        this.frontedConnection = service.getConnection();
        this.holder = holder;
    }

    public void execute(PhysicalDbGroup rwGroup) throws IOException {
        PhysicalDbInstance instance = rwGroup.select(true, true);
        instance.getConnection(rwSplitService.getSchema(), this, null, true);
    }

    @Override
    public void connectionAcquired(BackendConnection conn) {
        MySQLResponseService mysqlService = conn.getBackendService();
        mysqlService.setResponseHandler(this);
        mysqlService.execute(rwSplitService, holder.getPrepareOrigin());
    }

    @Override
    public void preparedOkResponse(byte[] ok, List<byte[]> fields, List<byte[]> params, MySQLResponseService service) {
        originStatementId = ByteUtil.readUB4(ok, 5);
        ByteUtil.writeUB4(holder.getExecuteOrigin(), originStatementId, 5);
        int length = ByteUtil.readUB3(holder.getExecuteOrigin(), 0);
        int paramsCount = holder.getParamsCount();
        int nullBitMapSize = (paramsCount + 7) / 8;
        byte[] originExecuteByte = holder.getExecuteOrigin();
        if (holder.isNeedAddFieldType()) {
            length += paramsCount * 2;
            ByteBuffer buffer = service.allocate(originExecuteByte.length + paramsCount * 2);
            buffer.put(originExecuteByte, 0, 14 + nullBitMapSize);
            ByteUtil.writeUB3(buffer, length, 0);
            //flag type
            buffer.put(14 + nullBitMapSize, (byte) 1);
            //field type
            buffer.position(15 + nullBitMapSize);
            byte[] fileType = holder.getFieldType();
            buffer.put(fileType, 0, fileType.length);
            buffer.position(15 + nullBitMapSize + paramsCount * 2);
            buffer.put(originExecuteByte, 15 + nullBitMapSize, originExecuteByte.length - (15 + nullBitMapSize));
            service.setResponseHandler(this);
            service.execute(buffer);
        } else {
            service.setResponseHandler(this);
            service.execute(holder.getExecuteOrigin());
        }
    }

    @Override
    public void connectionError(Throwable e, Object attachment) {
        rwSplitService.writeErrMessage(ErrorCode.ER_DB_INSTANCE_ABORTING_CONNECTION, "can't connect to dbGroup[" + rwSplitService.getUserConfig().getDbGroup());
    }

    @Override
    public void errorResponse(byte[] err, AbstractService service) {
        synchronized (this) {
            if (!write2Client) {
                err[3] = (byte) rwSplitService.nextPacketId();
                frontedConnection.getService().writeDirectly(err);
                write2Client = true;
                PreparedClosePacket close = new PreparedClosePacket(originStatementId);
                close.bufferWrite(service.getConnection());
                ((MySQLResponseService) service).release();
            }
        }
    }

    @Override
    public void okResponse(byte[] ok, AbstractService service) {
        MySQLResponseService mysqlService = (MySQLResponseService) service;
        boolean executeResponse = mysqlService.syncAndExecute();
        if (executeResponse) {
            synchronized (this) {
                if (!write2Client) {
                    frontedConnection.getService().writeDirectly(ok);
                    write2Client = true;
                    PreparedClosePacket close = new PreparedClosePacket(originStatementId);
                    close.bufferWrite(service.getConnection());
                    ((MySQLResponseService) service).release();
                }
            }
        }
    }

    @Override
    public void connectionClose(AbstractService service, String reason) {
        rwSplitService.getConnection().close("backend connection is close in ps");
    }

    @Override
    public void preparedExecuteResponse(byte[] header, List<byte[]> fields, byte[] eof, MySQLResponseService service) {
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof, boolean isLeft, AbstractService service) {

    }

    @Override
    public boolean rowResponse(byte[] rowNull, RowDataPacket rowPacket, boolean isLeft, AbstractService service) {
        return false;
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, AbstractService service) {

    }

}
