/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.backend.pool;


import com.actiontech.dble.net.connection.PooledConnection;

/**
 * @author collapsar
 */
public interface PooledConnectionListener {

    void onCreateSuccess(PooledConnection conn);

    void onCreateFail(PooledConnection conn, Throwable e);

    void onHeartbeatSuccess(PooledConnection conn);
}
