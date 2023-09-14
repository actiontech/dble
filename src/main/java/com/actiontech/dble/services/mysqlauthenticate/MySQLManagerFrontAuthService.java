/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.mysqlauthenticate;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.service.NotificationServiceTask;
import com.actiontech.dble.net.service.ServiceTask;


/**
 * Created by szf on 2020/6/18.
 */
public class MySQLManagerFrontAuthService extends MySQLFrontAuthService {

    public MySQLManagerFrontAuthService(AbstractConnection connection) {
        super(connection);
    }

    @Override
    public void handle(ServiceTask task) {
        beforeInsertServiceTask(task);
        task.setTaskId(taskId.getAndIncrement());
        DbleServer.getInstance().getManagerFrontHandlerQueue().offer(task);
    }

    public void notifyTaskThread() {
        DbleServer.getInstance().getManagerFrontHandlerQueue().offerFirst(new NotificationServiceTask(this));
    }
}
