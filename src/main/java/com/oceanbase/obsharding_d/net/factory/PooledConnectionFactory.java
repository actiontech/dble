/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.net.factory;


import com.oceanbase.obsharding_d.backend.mysql.nio.handler.ResponseHandler;
import com.oceanbase.obsharding_d.backend.pool.PooledConnectionListener;
import com.oceanbase.obsharding_d.backend.pool.ReadTimeStatusInstance;
import com.oceanbase.obsharding_d.net.connection.PooledConnection;

import java.io.IOException;


public abstract class PooledConnectionFactory {

    public abstract PooledConnection make(ReadTimeStatusInstance instance, ResponseHandler handler, String schema) throws IOException;

    public abstract PooledConnection make(ReadTimeStatusInstance instance, PooledConnectionListener listener, String schema) throws IOException;

}
