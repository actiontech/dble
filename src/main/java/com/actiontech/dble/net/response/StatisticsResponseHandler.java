package com.actiontech.dble.net.response;

import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.services.rwsplit.StatisticsHandler;

public class StatisticsResponseHandler extends CustomDataResponseHandler {

    public StatisticsResponseHandler(MySQLResponseService service) {
        super(service);
    }

    @Override
    public void data(byte[] data) {
        // COM_STATISTICS
        if (service.getSession2() != null) {
            ResponseHandler respHand = service.getResponseHandler();
            if (respHand instanceof StatisticsHandler) {
                ((StatisticsHandler) respHand).stringEof(data, service);
            } else {
                if (respHand == null) {
                    closeNoHandler();
                }
            }
        } else {
            // ignore
        }
    }
}
