/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

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
    protected void doHandle(ServiceTask task) {
        if (isComplexQuery()) {
            super.doHandle(null);
        } else {
            if (task == null) return;
            if (isHandling.compareAndSet(false, true)) {
                DbleServer.getInstance().getConcurrentBackHandlerQueue().offer(task);
            }
        }
    }

}
