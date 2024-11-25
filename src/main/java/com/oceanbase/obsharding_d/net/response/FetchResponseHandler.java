/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.net.response;

import com.oceanbase.obsharding_d.backend.mysql.nio.handler.ResponseHandler;
import com.oceanbase.obsharding_d.services.mysqlsharding.MySQLResponseService;

public class FetchResponseHandler implements ProtocolResponseHandler {

    private final MySQLResponseService service;

    public FetchResponseHandler(MySQLResponseService service) {
        this.service = service;
    }

    @Override
    public void ok(byte[] data) {
        handleRowPacket(data);
        service.setRowDataFlowing(true);
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
        handleRowEofPacket(eof);
    }

    @Override
    public void data(byte[] data) {
        handleRowPacket(data);
    }

    private void closeNoHandler() {
        if (!service.getConnection().isClosed()) {
            service.getConnection().close("no handler");
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
        ResponseHandler respHand = service.getResponseHandler();
        if (respHand != null) {
            service.backendSpecialCleanUp();
            respHand.rowEofResponse(data, false, service);
        } else {
            closeNoHandler();
        }
    }

}
