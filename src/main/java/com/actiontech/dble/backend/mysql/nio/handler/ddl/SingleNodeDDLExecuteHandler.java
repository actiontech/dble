package com.actiontech.dble.backend.mysql.nio.handler.ddl;

import com.actiontech.dble.log.transaction.TxnLogHelper;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.server.NonBlockingSession;
import org.jetbrains.annotations.Nullable;

public class SingleNodeDDLExecuteHandler extends BaseDDLHandler {

    public SingleNodeDDLExecuteHandler(NonBlockingSession session, RouteResultset rrs, @Nullable Object attachment) {
        super(session, rrs, attachment);
        TxnLogHelper.putTxnLog(session.getShardingService(), this.rrs);
    }
}
