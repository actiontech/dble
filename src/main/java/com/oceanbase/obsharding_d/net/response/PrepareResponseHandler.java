/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.net.response;

import com.oceanbase.obsharding_d.backend.mysql.ByteUtil;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.PreparedResponseHandler;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.ResponseHandler;
import com.oceanbase.obsharding_d.services.mysqlsharding.MySQLResponseService;

import java.util.ArrayList;
import java.util.List;

public class PrepareResponseHandler implements ProtocolResponseHandler {

    private volatile int status = PREPARED_FIELD;
    private volatile byte[] ok;
    private volatile List<byte[]> fields;
    private volatile List<byte[]> params;

    private final MySQLResponseService service;

    public PrepareResponseHandler(MySQLResponseService service) {
        this.service = service;
    }

    @Override
    public void ok(byte[] data) {
        boolean executeResponse = service.syncAndExecute();
        if (executeResponse) {
            final int fieldCount = (int) ByteUtil.readLength(data, 9);
            final int paramCount = (int) ByteUtil.readLength(data, 11);
            if (fieldCount > 0) {
                fields = new ArrayList<>(fieldCount + 1);
                status = PREPARED_FIELD;
            }
            if (paramCount > 0) {
                params = new ArrayList<>(paramCount + 1);
                status = PREPARED_PARAM;
            }
            if (fieldCount == 0 && paramCount == 0) {
                // handle ok packet
                handleOkPacket(data);
                return;
            }
            ok = data;
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
        if (status == PREPARED_FIELD) {
            fields.add(eof);
            // handle field eof
            handleOkPacket(ok);
        } else {
            params.add(eof);
            if (fields != null) {
                status = PREPARED_FIELD;
            } else {
                // handle param eof
                handleOkPacket(ok);
            }
        }
    }

    @Override
    public void data(byte[] data) {
        if (status == PREPARED_FIELD) {
            fields.add(data);
        } else {
            params.add(data);
        }
    }

    private void handleOkPacket(byte[] okPacket) {
        ResponseHandler respHand = service.getResponseHandler();
        if (respHand instanceof PreparedResponseHandler) {
            ((PreparedResponseHandler) respHand).preparedOkResponse(okPacket, fields, params, service);
            ok = null;
            params = null;
            fields = null;
            status = PREPARED_FIELD;
        } else {
            closeNoHandler();
        }
    }

    private void closeNoHandler() {
        if (!service.getConnection().isClosed()) {
            service.getConnection().close("no handler");
        }
    }

}
