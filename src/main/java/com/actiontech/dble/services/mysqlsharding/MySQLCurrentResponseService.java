package com.actiontech.dble.services.mysqlsharding;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.service.ServiceTask;

/**
 * Created by szf on 2020/7/9.
 */
public class MySQLCurrentResponseService extends MySQLResponseService {

    public MySQLCurrentResponseService(AbstractConnection connection) {
        super(connection);
    }


    @Override
    public void taskToTotalQueue(ServiceTask task) {
        if (isComplexQuery()) {
            super.taskToTotalQueue(task);
        } else {
            if (isHandling.compareAndSet(false, true)) {
                DbleServer.getInstance().getConcurrentBackHandlerQueue().offer(task);
            }
        }
    }


    @Override
    public void consumerInternalData() {
        try {
            super.handleInnerData();
        } finally {
            isHandling.set(false);
        }
    }
}
