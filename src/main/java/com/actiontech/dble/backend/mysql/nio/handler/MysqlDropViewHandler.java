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
import com.actiontech.dble.singleton.TraceManager;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLNonTransientException;

/**
 * @Author collapsar
 */
public class MysqlDropViewHandler extends MultiNodeDDLExecuteHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlDropViewHandler.class);
    private ViewMeta vm; //if only for replace from a no sharding view to a sharding view

    public MysqlDropViewHandler(NonBlockingSession session, RouteResultset rrs, ViewMeta vm) {
        super(rrs, session);
        this.vm = vm;
    }

    @Override
    public void execute() throws Exception {
        super.execute();
    }

    @Override
    public void okResponse(byte[] data, @NotNull AbstractService service) {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(service, "get-ok-response");
        TraceManager.finishSpan(service, traceObject);
        boolean executeResponse = ((MySQLResponseService) service).syncAndExecute();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("received ok response ,executeResponse:" + executeResponse + " from " + service);
        }
        if (executeResponse) {
            DDLTraceManager.getInstance().updateConnectionStatus(session.getShardingService(),
                    (MySQLResponseService) service, DDLTraceInfo.DDLConnectionStatus.CONN_EXECUTE_SUCCESS);
            session.setBackendResponseEndTime((MySQLResponseService) service);
            lock.lock();
            try {
                ShardingService source = session.getShardingService();
                if (!decrementToZero((MySQLResponseService) service))
                    return;
                if (isFail()) {
                    DDLTraceManager.getInstance().endDDL(source, "ddl end with execution failure");
                    session.resetMultiStatementStatus();
                    handleEndPacket(err, false);
                } else {
                    if (vm != null) {
                        try {
                            vm.addMeta(true);
                        } catch (SQLNonTransientException e) {
                            DDLTraceManager.getInstance().endDDL(session.getShardingService(), "ddl end with meta failure");
                            session.resetMultiStatementStatus();
                            executeMetaDataFailed(e.getMessage());
                            return;
                        }
                    }
                    session.setRowCount(0);
                    DDLTraceManager.getInstance().endDDL(source, null);
                    OkPacket ok = new OkPacket();
                    ok.read(data);
                    ok.setMessage(null);
                    ok.setAffectedRows(0);
                    ok.setServerStatus(source.isAutocommit() ? 2 : 1);
                    doSqlStat();
                    handleEndPacket(ok, true);
                }
            } finally {
                lock.unlock();
            }
        }
    }

}
