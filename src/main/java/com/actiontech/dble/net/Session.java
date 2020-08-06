/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.net;

import com.actiontech.dble.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.actiontech.dble.backend.mysql.store.memalloc.MemSizeController;
import com.actiontech.dble.net.connection.FrontendConnection;
import com.actiontech.dble.route.RouteResultsetNode;

public abstract class Session {

    /**
     * get frontend conn
     */
    public abstract FrontendConnection getSource();

    public void setHandlerStart(DMLResponseHandler handler) {

    }

    public void setHandlerEnd(DMLResponseHandler handler) {

    }

    public void onQueryError(byte[] message) {

    }

    public MemSizeController getJoinBufferMC() {
        return null;
    }

    public MemSizeController getOrderBufferMC() {
        return null;
    }

    public MemSizeController getOtherBufferMC() {
        return null;
    }

    public void setRouteResultToTrace(RouteResultsetNode[] nodes) {

    }

    public void allBackendConnReceive() {
    }
}
