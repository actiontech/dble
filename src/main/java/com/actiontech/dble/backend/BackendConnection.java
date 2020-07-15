/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend;

import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.backend.pool.PooledEntry;
import com.actiontech.dble.net.ClosableConnection;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.ServerConnection;

import java.io.UnsupportedEncodingException;

public interface BackendConnection extends ClosableConnection, PooledEntry {

    boolean isDDL();

    boolean isFromSlaveDB();

    String getSchema();

    void setSchema(String newSchema);

    long getLastTime();

    void setAttachment(Object attachment);

    void ping();

    void connect();

    boolean setResponseHandler(ResponseHandler commandHandler);

    void setSession(NonBlockingSession session);

    void setDbInstance(PhysicalDbInstance instance);

    void commit();

    void query(String sql) throws UnsupportedEncodingException;

    void query(String query, boolean isAutocommit);

    Object getAttachment();

    void execute(RouteResultsetNode node, ServerConnection source,
                 boolean autocommit);

    boolean syncAndExecute();

    void rollback();

    int getTxIsolation();

    boolean isAutocommit();

    long getId();

    void closeWithoutRsp(String reason);

    String compactInfo();

    void setOldTimestamp(long oldTimestamp);

    void setExecuting(boolean executing);

    boolean isExecuting();

    void disableRead();

    void enableRead();
}
