/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.net.response;

import com.oceanbase.obsharding_d.backend.mysql.ByteUtil;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.ResponseHandler;
import com.oceanbase.obsharding_d.services.mysqlsharding.MySQLResponseService;
import com.oceanbase.obsharding_d.services.rwsplit.ShowFieldsHandler;

import java.util.ArrayList;
import java.util.List;

public class FieldListResponseHandler implements ProtocolResponseHandler {

    private volatile int status = INITIAL;
    protected volatile byte[] header;
    protected volatile List<byte[]> fields;
    private final MySQLResponseService service;

    public FieldListResponseHandler(MySQLResponseService service) {
        this.service = service;
    }

    @Override
    public void ok(byte[] data) {
        if (status == FIELD) {
            fields.add(data);
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
            ResponseHandler respHand = service.getResponseHandler();
            if (respHand instanceof ShowFieldsHandler) {
                ((ShowFieldsHandler) respHand).fieldsEof(header, fields, eof, service);
            } else {
                closeNoHandler();
            }
        }
    }

    @Override
    public void data(byte[] data) {
        if (status == INITIAL) {
            if (service.getSession() != null) {
                service.getSession().startExecuteBackend();
            }
            status = FIELD;
            header = data;
            fields = new ArrayList<>((int) ByteUtil.readLength(data, 4));
        } else if (status == FIELD) {
            fields.add(data);
        }
    }

    private void closeNoHandler() {
        if (!service.getConnection().isClosed()) {
            service.getConnection().close("no handler");
        }
    }

}
