/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.net.response;

import com.oceanbase.obsharding_d.backend.mysql.ByteUtil;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.ResponseHandler;
import com.oceanbase.obsharding_d.net.mysql.MySQLPacket;
import com.oceanbase.obsharding_d.net.mysql.RequestFilePacket;
import com.oceanbase.obsharding_d.services.mysqlsharding.MySQLResponseService;

import java.util.ArrayList;

public class LoadDataResponseHandler extends CustomDataResponseHandler {
    private volatile int status = INITIAL;

    public LoadDataResponseHandler(MySQLResponseService service) {
        super(service);
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
        beforeError();
        if (respHand != null) {
            respHand.errorResponse(data, service);
        } else {
            closeNoHandler();
        }
    }

    @Override
    protected void beforeError() {
        service.releaseSignal();
        status = INITIAL;
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
        if (respHand instanceof com.oceanbase.obsharding_d.backend.mysql.nio.handler.LoadDataResponseHandler) {
            ((com.oceanbase.obsharding_d.backend.mysql.nio.handler.LoadDataResponseHandler) respHand).requestDataResponse(data, service);
        } else {
            closeNoHandler();
        }
    }

    private void handleRowEofPacket(byte[] data) {
        if (service.getSession() != null && !service.isTesting() && service.getLogResponse().compareAndSet(false, true)) {
            service.getSession().setBackendResponseEndTime(this.service);
        }
        ResponseHandler respHand = service.getResponseHandler();
        service.getLogResponse().set(false);
        if (respHand != null) {
            service.backendSpecialCleanUp();
            respHand.rowEofResponse(data, false, service);
        } else {
            closeNoHandler();
        }
    }

}
