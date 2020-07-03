package com.actiontech.dble.services.factorys;


import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.backend.pool.PooledConnectionListener;
import com.actiontech.dble.backend.pool.ReadTimeStatusInstance;
import com.actiontech.dble.net.connection.PooledConnection;
import com.actiontech.dble.net.factory.PooledConnectionFactory;

import java.io.IOException;

/**
 * Created by szf on 2020/6/29.
 */
public class MySQLConnectionFactory extends PooledConnectionFactory {

    @Override
    public PooledConnection make(ReadTimeStatusInstance instance, ResponseHandler handler, String schema) throws IOException {
        return null;
    }

    @Override
    public PooledConnection make(ReadTimeStatusInstance instance, PooledConnectionListener listener, String schema) throws IOException {
        return null;
    }
}
