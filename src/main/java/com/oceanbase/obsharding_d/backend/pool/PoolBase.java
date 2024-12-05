/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.pool;

import com.oceanbase.obsharding_d.backend.mysql.nio.handler.ResponseHandler;
import com.oceanbase.obsharding_d.config.model.db.DbInstanceConfig;
import com.oceanbase.obsharding_d.net.connection.PooledConnection;
import com.oceanbase.obsharding_d.net.factory.PooledConnectionFactory;

import java.io.IOException;

public class PoolBase {

    protected DbInstanceConfig config;
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

    void newConnection(String schema, PooledConnectionListener listener, boolean createByWaiter) {
        try {
            PooledConnection pooledConnection = factory.make(instance, listener, schema);
            pooledConnection.getCreateByWaiter().set(createByWaiter);
        } catch (IOException ioe) {
            listener.onCreateFail(null, ioe);
        }
    }
}
