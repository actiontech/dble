/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.backend.mysql.nio;

import com.actiontech.dble.backend.BackendConnection;

/**
 * @author collapsar
 */
public interface MySQLConnectionListener {

    void onSuccess(BackendConnection conn);

    void onError(BackendConnection conn, Throwable e);
}
