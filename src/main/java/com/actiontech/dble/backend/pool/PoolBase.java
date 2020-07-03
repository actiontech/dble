package com.actiontech.dble.backend.pool;

import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.config.model.db.DbInstanceConfig;
import com.actiontech.dble.net.connection.PooledConnection;
import com.actiontech.dble.net.factory.PooledConnectionFactory;

import java.io.IOException;

public class PoolBase {

    protected final DbInstanceConfig config;
    protected final ReadTimeStatusInstance instance;
    protected final PooledConnectionFactory factory;

    public PoolBase(DbInstanceConfig dbConfig, ReadTimeStatusInstance instance, PooledConnectionFactory factory) {
        this.config = dbConfig;
        this.instance = instance;
        this.factory = factory;
    }

    /**
     * only for heartbeat
     *
     * @return
     */
    public void newConnection(String schema, ResponseHandler handler) {
        try {
            factory.make(instance, handler, schema);
        } catch (IOException ioe) {
            handler.connectionError(ioe, null);
        }
    }

    PooledConnection newConnection(String schema, PooledConnectionListener listener) {
        PooledConnection conn = null;
        try {
            return factory.make(instance, listener, schema);
        } catch (IOException ioe) {
            listener.onCreateFail(conn, ioe);
            return null;
        }
    }
}
