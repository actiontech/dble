/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.cluster.values.DDLTraceInfo;
import com.actiontech.dble.meta.ViewMeta;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.singleton.DDLTraceManager;
import org.jetbrains.annotations.NotNull;

/**
 * @Author collapsar
 */
public class MysqlCreateViewHandler extends SingleNodeDDLHandler {
    private ViewMeta vm;

    public MysqlCreateViewHandler(NonBlockingSession session, RouteResultset rrs, ViewMeta vm) {
        super(rrs, session);
        this.vm = vm;
    }

    @Override
    public void execute() throws Exception {
        super.execute();
    }

    @Override
    public void okResponse(byte[] data, @NotNull AbstractService service) {
        boolean executeResponse = ((MySQLResponseService) service).syncAndExecute();
        if (executeResponse) {
            DDLTraceManager.getInstance().updateConnectionStatus(session.getShardingService(), (MySQLResponseService) service, DDLTraceInfo.DDLConnectionStatus.CONN_EXECUTE_SUCCESS);
            if (vm != null) {
                try {
                    vm.addMeta(true);
                } catch (Exception e) {
                    DDLTraceManager.getInstance().endDDL(session.getShardingService(), "ddl end with meta failure");
                    executeMetaDataFailed((MySQLResponseService) service, e.getMessage());
                    return;
                }
            }
            DDLTraceManager.getInstance().endDDL(session.getShardingService(), null);
            session.setRowCount(0);
            ShardingService sessionShardingService = session.getShardingService();
            OkPacket ok = new OkPacket();
            ok.read(data);
            ok.setPacketId(sessionShardingService.nextPacketId()); // OK_PACKET
            ok.setMessage(null);
            ok.setServerStatus(sessionShardingService.isAutocommit() ? 2 : 1);
            sessionShardingService.setLastInsertId(ok.getInsertId());
            session.setBackendResponseEndTime((MySQLResponseService) service);
            session.releaseConnectionIfSafe((MySQLResponseService) service, false);
            session.multiStatementPacket(ok);
            if (writeToClient.compareAndSet(false, true)) {
                handleEndPacket(ok, true);
            }
        }
    }

}
