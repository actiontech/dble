/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.net.factory;


import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.backend.pool.PooledConnectionListener;
import com.actiontech.dble.backend.pool.ReadTimeStatusInstance;
import com.actiontech.dble.net.connection.PooledConnection;

import java.io.IOException;


public abstract class PooledConnectionFactory {

    public abstract PooledConnection make(ReadTimeStatusInstance instance, ResponseHandler handler, String schema) throws IOException;

    public abstract PooledConnection make(ReadTimeStatusInstance instance, PooledConnectionListener listener, String schema) throws IOException;

}
