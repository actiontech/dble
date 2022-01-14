package com.actiontech.dble.backend.mysql.nio.handler.ddl;

import com.actiontech.dble.log.transaction.TxnLogHelper;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.singleton.DDLTraceHelper;
import org.jetbrains.annotations.Nullable;

public class MultiNodeDDLExecuteHandler extends BaseDDLHandler {

    public MultiNodeDDLExecuteHandler(NonBlockingSession session, RouteResultset rrs, @Nullable Object attachment) {
        super(session, rrs, attachment);
        TxnLogHelper.putTxnLog(session.getShardingService(), this.rrs);
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
