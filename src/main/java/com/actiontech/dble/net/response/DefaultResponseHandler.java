package com.actiontech.dble.net.response;

import com.actiontech.dble.backend.mysql.ByteUtil;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.net.mysql.MySQLPacket;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;

import java.util.ArrayList;
import java.util.List;

/**
 * for select and dml response packet
 *
 * @author collapsar
 */
public class DefaultResponseHandler implements ProtocolResponseHandler {

    private volatile int status = HEADER;
    protected volatile byte[] header;
    protected volatile List<byte[]> fields;

    protected MySQLResponseService service;

    public DefaultResponseHandler(MySQLResponseService service) {
        this.service = service;
    }

    @Override
    public void ok(byte[] data) {
        if (status != ROW) {
            ResponseHandler respHand = service.getResponseHandler();
            if (respHand != null) {
                respHand.okResponse(data, service);
            }
        } else {
            handleRowPacket(data);
        }
    }

    @Override
    public void error(byte[] data) {
        final ResponseHandler respHand = service.getResponseHandler();
        service.backendSpecialCleanUp();
        if (respHand != null) {
            respHand.errorResponse(data, service);
        } else {
            closeNoHandler();
        }
    }

    @Override
    public void eof(byte[] eof) {
        if (status == FIELD) {
            status = ROW;
            handleFieldEofPacket(eof);
        } else if (eof.length > MySQLPacket.MAX_EOF_SIZE) {
            handleRowPacket(eof);
        } else {
            status = HEADER;
            handleRowEofPacket(eof);
        }
    }

    @Override
    public void data(byte[] data) {
        if (status == HEADER) {
            status = FIELD;
            header = data;
            fields = new ArrayList<>((int) ByteUtil.readLength(data, 4));
        } else if (status == FIELD) {
            fields.add(data);
        } else {
            handleRowPacket(data);
        }
    }

    protected void closeNoHandler() {
        if (!service.getConnection().isClosed()) {
            service.getConnection().close("no handler");
        }
    }

    private void handleFieldEofPacket(byte[] data) {
        ResponseHandler respHand = service.getResponseHandler();
        service.setRowDataFlowing(true);
        if (respHand != null) {
            respHand.fieldEofResponse(header, fields, null, data, false, service);
        } else {
            closeNoHandler();
        }
    }

    private void handleRowPacket(byte[] data) {
        //LOGGER.info("get into rowing data " + data.length);
        ResponseHandler respHand = service.getResponseHandler();
        if (respHand != null) {
            respHand.rowResponse(data, null, false, service);
        } else {
            closeNoHandler();
        }
    }

    private void handleRowEofPacket(byte[] data) {
        if (service.getSession() != null && !service.isTesting() && service.getLogResponse().compareAndSet(false, true)) {
            service.getSession().setBackendResponseEndTime(this.service);
        }
        service.getLogResponse().set(false);
        service.backendSpecialCleanUp();
        if (service.getResponseHandler() != null) {
            service.getResponseHandler().rowEofResponse(data, false, service);
        } else {
            closeNoHandler();
        }
    }

}
