/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * UnLock Tables Handler
 *
 * @author songdabin
 */
public class UnLockTablesHandler extends DefaultMultiNodeHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(UnLockTablesHandler.class);

    private final boolean autocommit;
    private final String srcStatement;

    public UnLockTablesHandler(NonBlockingSession session, boolean autocommit, String sql) {
        super(session);
        this.autocommit = autocommit;
        this.srcStatement = sql;
    }

    public void execute() {
        Map<RouteResultsetNode, BackendConnection> lockedCons = session.getTargetMap();
        this.reset();
        // if client just send an unlock tables, theres is no lock tables statement, just send back OK
        if (lockedCons.size() == 0) {
            LOGGER.info("find no locked backend connection!" + session.getSource());
            OkPacket ok = new OkPacket();
            ok.setPacketId(session.getShardingService().nextPacketId());
            ok.setPacketLength(7); // the size of unlock table's response OK packet is 7
            ok.setServerStatus(session.getShardingService().isAutocommit() ? 2 : 1);
            ok.write(session.getSource());
            return;
        }
        Map<RouteResultsetNode, BackendConnection> forUnlocks = new HashMap<>(lockedCons.size());
        for (Map.Entry<RouteResultsetNode, BackendConnection> entry : lockedCons.entrySet()) {
            RouteResultsetNode shardingNode = entry.getKey();
            BackendConnection conn = entry.getValue();
            RouteResultsetNode node = new RouteResultsetNode(shardingNode.getName(), ServerParse.UNLOCK, srcStatement);
            forUnlocks.put(node, conn);
            unResponseRrns.add(node);
        }
        for (Map.Entry<RouteResultsetNode, BackendConnection> entry : forUnlocks.entrySet()) {
            RouteResultsetNode node = entry.getKey();
            BackendConnection conn = entry.getValue();
            if (clearIfSessionClosed(session)) {
                return;
            }
            conn.getBackendService().setResponseHandler(this);
            conn.getBackendService().setSession(session);
            try {
                conn.getBackendService().execute(node, session.getShardingService(), autocommit);
            } catch (Exception e) {
                connectionError(e, node);
            }
        }
    }

    @Override
    public void handleErrorResponse(ErrorPacket err, @NotNull AbstractService service) {
        session.releaseConnectionIfSafe(((MySQLResponseService) service), false);
    }

    @Override
    public void handleOkResponse(byte[] data, @NotNull AbstractService service) {
        session.releaseConnection(((MySQLResponseService) service).getConnection());
    }

    @Override
    protected void finish(byte[] ok) {
        if (this.isFail()) {
            this.tryErrorFinished(true);
            return;
        }
        OkPacket okPacket = new OkPacket();
        okPacket.read(ok);
        okPacket.setPacketId(session.getShardingService().nextPacketId());
        okPacket.setServerStatus(session.getShardingService().isAutocommit() ? 2 : 1);
        okPacket.write(session.getSource());
    }

}
