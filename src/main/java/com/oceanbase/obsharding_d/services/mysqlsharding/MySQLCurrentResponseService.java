/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.mysqlsharding;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.net.connection.BackendConnection;
import com.oceanbase.obsharding_d.net.service.ServiceTask;

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
                OBsharding_DServer.getInstance().getConcurrentBackHandlerQueue().offer(task);
            }
        }
    }

}
