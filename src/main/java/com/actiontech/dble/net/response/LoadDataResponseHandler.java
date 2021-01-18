package com.actiontech.dble.net.response;

import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.net.mysql.RequestFilePacket;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;

public class LoadDataResponseHandler extends DefaultResponseHandler {

    public LoadDataResponseHandler(MySQLResponseService service) {
        super(service);
    }

    @Override
    public void data(byte[] data) {
        if (data[4] == RequestFilePacket.FIELD_COUNT) {
            handleRequestPacket(data);
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
}
