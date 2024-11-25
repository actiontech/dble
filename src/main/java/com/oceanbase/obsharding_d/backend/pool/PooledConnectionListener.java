/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.backend.pool;


import com.oceanbase.obsharding_d.net.connection.PooledConnection;

/**
 * @author collapsar
 */
public interface PooledConnectionListener {

    void onCreateSuccess(PooledConnection conn);

    void onCreateFail(PooledConnection conn, Throwable e);

    void onHeartbeatSuccess(PooledConnection conn);
}
