/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.net.response;

import com.oceanbase.obsharding_d.backend.mysql.nio.handler.ResponseHandler;
import com.oceanbase.obsharding_d.services.mysqlsharding.MySQLResponseService;
import com.oceanbase.obsharding_d.services.rwsplit.StatisticsHandler;

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
