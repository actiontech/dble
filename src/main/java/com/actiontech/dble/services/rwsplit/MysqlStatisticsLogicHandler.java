package com.actiontech.dble.services.rwsplit;

import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.services.mysqlsharding.MysqlBackendLogicHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MysqlStatisticsLogicHandler extends MysqlBackendLogicHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlStatisticsLogicHandler.class);

    public MysqlStatisticsLogicHandler(MySQLResponseService service) {
        super(service);
    }

    public void handleInnerData(byte[] data) {
        // COM_STATISTICS
        service.resetStatisticResponse();
        ResponseHandler respHand = service.getResponseHandler();
        if (respHand instanceof StatisticsHandler) {
            ((StatisticsHandler) respHand).stringEof(data, service);
        } else {
            if (respHand == null) {
                closeNoHandler();
            }
        }

    }

    private void closeNoHandler() {
        if (!service.getConnection().isClosed()) {
            service.getConnection().close("no handler");
            LOGGER.info("no handler bind in this con " + this + " client:" + service);
        }
    }

    public void reset() {
        super.reset();
        service.resetStatisticResponse();
    }
}
