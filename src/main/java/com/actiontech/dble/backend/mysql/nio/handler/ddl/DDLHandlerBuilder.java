/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.ddl;

import com.actiontech.dble.meta.ViewMeta;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.singleton.DDLTraceHelper;

public final class DDLHandlerBuilder {

    private DDLHandlerBuilder() {
    }

    public static BaseDDLHandler build(NonBlockingSession session, RouteResultset rrs, ImplicitlyCommitCallback implicitlyCommitCallback) {
        if (rrs.getNodes().length == 1) {
            return new SingleNodeDDLExecuteHandler(session, rrs, null, implicitlyCommitCallback);
        } else {
            return new MultiNodeDdlPrepareHandler(session, rrs, null, implicitlyCommitCallback);
        }
    }

    public static BaseDDLHandler buildView(NonBlockingSession session, RouteResultset rrs, ViewMeta vm, ImplicitlyCommitCallback implicitlyCommitCallback) {
        if (rrs.getNodes().length == 1) {
            return new SingleNodeViewHandler(session, rrs, vm, implicitlyCommitCallback);
        } else {
            return new MultiNodeViewHandler(session, rrs, vm, implicitlyCommitCallback);
        }
    }

    // In mysql drop\create view
    // single node
    static class SingleNodeViewHandler extends SingleNodeDDLExecuteHandler {
        SingleNodeViewHandler(NonBlockingSession session, RouteResultset rrs, ViewMeta vm, ImplicitlyCommitCallback implicitlyCommitCallback) {
            super(session, rrs, vm, implicitlyCommitCallback);
        }

        @Override
        protected boolean specialHandling0(boolean isExecSucc) {
            ShardingService shardingService = session.getShardingService();
            if (!isExecSucc) { // execute only when okResponse
                return true;
            }
            if (attachment != null && attachment instanceof ViewMeta) {
                ViewMeta vm = (ViewMeta) attachment;
                DDLTraceHelper.log(shardingService, d -> d.info(DDLTraceHelper.Stage.update_table_metadata, DDLTraceHelper.Status.start));
                try {
                    vm.addMeta(true);
                } catch (Exception e) {
                    DDLTraceHelper.log(shardingService, d -> d.info(DDLTraceHelper.Stage.update_table_metadata, DDLTraceHelper.Status.fail, e.getMessage()));
                    return false;
                }
                DDLTraceHelper.log(shardingService, d -> d.info(DDLTraceHelper.Stage.update_table_metadata, DDLTraceHelper.Status.succ));
            }
            return true;
        }
    }

    // multi node
    static class MultiNodeViewHandler extends MultiNodeDdlPrepareHandler {
        MultiNodeViewHandler(NonBlockingSession session, RouteResultset rrs, ViewMeta vm, ImplicitlyCommitCallback implicitlyCommitCallback) {
            super(session, rrs, vm, implicitlyCommitCallback);
        }

        @Override
        protected boolean specialHandling0(boolean isExecSucc) {
            ShardingService shardingService = session.getShardingService();
            if (!isExecSucc) { // execute only when okResponse
                return true;
            }
            if (attachment != null && attachment instanceof ViewMeta) {
                ViewMeta vm = (ViewMeta) attachment;
                DDLTraceHelper.log(shardingService, d -> d.info(DDLTraceHelper.Stage.update_table_metadata, DDLTraceHelper.Status.start));
                try {
                    vm.addMeta(true);
                } catch (Exception e) {
                    DDLTraceHelper.log(shardingService, d -> d.info(DDLTraceHelper.Stage.update_table_metadata, DDLTraceHelper.Status.fail, e.getMessage()));
                    return false;
                }
                DDLTraceHelper.log(shardingService, d -> d.info(DDLTraceHelper.Stage.update_table_metadata, DDLTraceHelper.Status.succ));
            }
            return true;
        }
    }
}
