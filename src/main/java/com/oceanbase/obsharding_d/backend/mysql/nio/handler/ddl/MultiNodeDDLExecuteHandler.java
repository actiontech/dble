/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.nio.handler.ddl;

import com.oceanbase.obsharding_d.net.connection.BackendConnection;
import com.oceanbase.obsharding_d.route.RouteResultset;
import com.oceanbase.obsharding_d.route.RouteResultsetNode;
import com.oceanbase.obsharding_d.server.NonBlockingSession;
import com.oceanbase.obsharding_d.singleton.DDLTraceHelper;
import org.jetbrains.annotations.Nullable;

public class MultiNodeDDLExecuteHandler extends BaseDDLHandler {

    public MultiNodeDDLExecuteHandler(NonBlockingSession session, RouteResultset rrs, @Nullable Object attachment, RouteResultset preRrs, ImplicitlyCommitCallback implicitlyCommitCallback) {
        super(session, rrs, attachment, implicitlyCommitCallback);
        this.preRrs = preRrs;
    }

    @Override
    protected void innerExecute(BackendConnection conn, RouteResultsetNode node) {
        //do ddl what ever the serverConnection is closed
        conn.getBackendService().setResponseHandler(this);
        if (conn.isClosed()) {
            conn.close("DDL find backendConnection close"); // will jump to connectionClose
            return;
        }
        DDLTraceHelper.log(session.getShardingService(), d -> d.infoByNode(node.getName(), stage, DDLTraceHelper.Status.get_conn, "Get " + conn.toString()));
        conn.getBackendService().setSession(session);
        conn.getBackendService().executeMultiNode(node, session.getShardingService(), sessionAutocommit && !session.getShardingService().isTxStart());
    }
}
