package com.actiontech.dble.services.mysqlsharding;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.service.ServiceTask;

/**
 * Created by szf on 2020/7/9.
 */
public class MySQLCurrentResponseService extends MySQLResponseService {

    public MySQLCurrentResponseService(BackendConnection connection) {
        super(connection);
    }

    @Override
    public void handle(ServiceTask task) {
        if (isComplexQuery()) {
            super.handle(task);
        } else {
            if (isHandling.compareAndSet(false, true)) {
                DbleServer.getInstance().getConcurrentBackHandlerQueue().offer(task);
            }
        }
    }

}
