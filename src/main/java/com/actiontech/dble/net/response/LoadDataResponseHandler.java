package com.actiontech.dble.net.response;

import com.actiontech.dble.backend.mysql.ByteUtil;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.net.mysql.MySQLPacket;
import com.actiontech.dble.net.mysql.RequestFilePacket;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;

import java.util.ArrayList;

public class LoadDataResponseHandler extends DefaultResponseHandler {
    private volatile int status = INITIAL;

    public LoadDataResponseHandler(MySQLResponseService service) {
        super(service);
    }

    @Override
    public void ok(byte[] data) {
        if (status == INITIAL) {
            ResponseHandler respHand = service.getResponseHandler();
            if (respHand != null) {
                respHand.okResponse(data, service);
            }
        } else if (status == FIELD) {
            fields.add(data);
        } else {
            handleRowPacket(data);
        }
    }

    @Override
    public void data(byte[] data) {
        if (data[4] == RequestFilePacket.FIELD_COUNT) {
            handleRequestPacket(data);
        } else if (status == INITIAL) {
            status = FIELD;
            header = data;
            fields = new ArrayList<>((int) ByteUtil.readLength(data, 4));
        } else if (status == FIELD) {
            fields.add(data);
        } else {
            handleRowPacket(data);
        }
    }

    @Override
    public void error(byte[] data) {
        final ResponseHandler respHand = service.getResponseHandler();
        service.setExecuting(false);
        if (status != INITIAL) {
            service.setRowDataFlowing(false);
            service.signal();
            status = INITIAL;
        }
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
            status = INITIAL;
            handleRowEofPacket(eof);
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
        ResponseHandler respHand = service.getResponseHandler();
        if (respHand != null) {
            respHand.rowResponse(data, null, false, service);
        } else {
            closeNoHandler();
        }
    }

    private void handleRequestPacket(byte[] data) {
        ResponseHandler respHand = service.getResponseHandler();
        if (respHand instanceof com.actiontech.dble.backend.mysql.nio.handler.LoadDataResponseHandler) {
            ((com.actiontech.dble.backend.mysql.nio.handler.LoadDataResponseHandler) respHand).requestDataResponse(data, service);
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
