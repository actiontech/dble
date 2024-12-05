/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.net.response;

import com.oceanbase.obsharding_d.backend.mysql.nio.handler.PreparedResponseHandler;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.ResponseHandler;
import com.oceanbase.obsharding_d.services.mysqlsharding.MySQLResponseService;

public class ExecuteResponseHandler extends DefaultResponseHandler {

    private final boolean cursor;
    private volatile boolean okAfterEof = false;

    public ExecuteResponseHandler(MySQLResponseService service, boolean cursor) {
        super(service);
        this.cursor = cursor;
    }

    @Override
    public void ok(byte[] data) {
        // if prepared statement doesn't keep cursor on, the response contains additional ok packet
        if (okAfterEof) {
            super.data(data);
        } else {
            super.ok(data);
        }
    }

    @Override
    public void eof(byte[] eof) {
        if (cursor) {
            handleFieldEofPacket(eof);
        } else {
            okAfterEof = true;
            super.eof(eof);
        }
    }

    private void handleFieldEofPacket(byte[] eof) {
        ResponseHandler respHand = service.getResponseHandler();
        if (respHand instanceof PreparedResponseHandler) {
            ((PreparedResponseHandler) respHand).preparedExecuteResponse(header, fields, eof, service);
        } else {
            closeNoHandler();
        }
    }

}
