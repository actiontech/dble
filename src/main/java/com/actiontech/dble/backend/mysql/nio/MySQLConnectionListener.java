/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.backend.mysql.nio;

import com.actiontech.dble.net.service.AbstractService;

/**
 * @author collapsar
 */
public interface MySQLConnectionListener {

    void onCreateSuccess(AbstractService service);

    void onCreateFail(AbstractService service, Throwable e);

    void onHeartbeatSuccess(AbstractService service);
}
