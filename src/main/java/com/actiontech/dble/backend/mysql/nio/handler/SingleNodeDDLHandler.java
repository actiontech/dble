package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.cluster.values.DDLTraceInfo;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.MySQLPacket;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.singleton.DDLTraceManager;
import com.actiontech.dble.util.StringUtil;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;

/**
 * Created by szf on 2019/12/3.
 */
public class SingleNodeDDLHandler extends SingleNodeHandler {

    public SingleNodeDDLHandler(RouteResultset rrs, NonBlockingSession session) {
        super(rrs, session);
    }

    public void execute() throws Exception {
        DDLTraceManager.getInstance().updateDDLStatus(DDLTraceInfo.DDLStage.EXECUTE_START, session.getShardingService());
        super.execute();
    }

    public void execute(MySQLResponseService service) {
        DDLTraceManager.getInstance().updateConnectionStatus(session.getShardingService(), service, DDLTraceInfo.DDLConnectionStatus.CONN_EXECUTE_START);
        super.execute(service.getConnection());
    }

    @Override
    public void connectionError(Throwable e, Object attachment) {
        DDLTraceManager.getInstance().updateRouteNodeStatus(session.getShardingService(),
                (RouteResultsetNode) attachment, DDLTraceInfo.DDLConnectionStatus.TEST_CONN_ERROR);
        DDLTraceManager.getInstance().endDDL(session.getShardingService(), e.getMessage());
        super.connectionError(e, attachment);
    }

    @Override
    public void errorResponse(byte[] data, @NotNull AbstractService service) {
        DDLTraceManager.getInstance().updateConnectionStatus(session.getShardingService(),
                (MySQLResponseService) service, DDLTraceInfo.DDLConnectionStatus.CONN_EXECUTE_ERROR);
        DDLTraceManager.getInstance().endDDL(session.getShardingService(), "ddl end with execution failure");
        super.errorResponse(data, service);
    }

    @Override
    public void connectionClose(@NotNull AbstractService service, String reason) {
        DDLTraceManager.getInstance().updateConnectionStatus(session.getShardingService(),
                (MySQLResponseService) service, DDLTraceInfo.DDLConnectionStatus.EXECUTE_CONN_CLOSE);
        DDLTraceManager.getInstance().endDDL(session.getShardingService(), reason);
        super.connectionClose(service, reason);
    }

    @Override
    public void okResponse(byte[] data, @NotNull AbstractService service) {
        boolean executeResponse = ((MySQLResponseService) service).syncAndExecute();
        if (executeResponse) {
            DDLTraceManager.getInstance().updateConnectionStatus(session.getShardingService(), (MySQLResponseService) service, DDLTraceInfo.DDLConnectionStatus.CONN_EXECUTE_SUCCESS);
            // handleSpecial
            boolean metaInitial = session.handleSpecial(rrs, true, null);
            if (!metaInitial) {
                DDLTraceManager.getInstance().endDDL(session.getShardingService(), "ddl end with meta failure");
                executeMetaDataFailed((MySQLResponseService) service, null);
            } else {
                DDLTraceManager.getInstance().endDDL(session.getShardingService(), null);
                session.setRowCount(0);
                ShardingService sessionShardingService = session.getShardingService();
                OkPacket ok = new OkPacket();
                ok.read(data);
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

    public void executeMetaDataFailed(MySQLResponseService service, String errMsg) {
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.setErrNo(ErrorCode.ER_META_DATA);
        if (errMsg == null) {
            errMsg = "Create TABLE OK, but generate metedata failed. The reason may be that the current druid parser can not recognize part of the sql" +
                    " or the user for backend mysql does not have permission to execute the heartbeat sql.";
        }
        errPacket.setMessage(StringUtil.encode(errMsg, session.getShardingService().getCharset().getResults()));
        session.setBackendResponseEndTime(service);
        session.releaseConnectionIfSafe(service, false);
        session.multiStatementPacket(errPacket);
        doSqlStat();
        if (writeToClient.compareAndSet(false, true)) {
            handleEndPacket(errPacket, false);
        }
    }

    @Override
    protected void backConnectionErr(ErrorPacket errPkg, @Nullable MySQLResponseService service, boolean syncFinished) {
        if (service != null && !service.isFakeClosed()) {
            if (service.getConnection().isClosed()) {
                if (service.getAttachment() != null) {
                    RouteResultsetNode rNode = (RouteResultsetNode) service.getAttachment();
                    session.getTargetMap().remove(rNode);
                }
            } else if (syncFinished) {
                session.releaseConnectionIfSafe(service, false);
            } else {
                service.getConnection().businessClose("unfinished sync");
                if (service.getAttachment() != null) {
                    RouteResultsetNode rNode = (RouteResultsetNode) service.getAttachment();
                    session.getTargetMap().remove(rNode);
                }
            }
        }

        ShardingService shardingService = session.getShardingService();
        String errMsg = "errNo:" + errPkg.getErrNo() + " " + new String(errPkg.getMessage());
        LOGGER.info("execute sql err:{}, con:{}, frontend host:{}/{}/{}", errMsg, service,
                shardingService.getConnection().getHost(),
                shardingService.getConnection().getLocalPort(),
                shardingService.getUser());

        if (writeToClient.compareAndSet(false, true)) {
            session.handleSpecial(rrs, false, null);
            handleEndPacket(errPkg, false);
        }
    }

    protected void handleEndPacket(MySQLPacket packet, boolean isSuccess) {
        session.clearResources(false);
        session.setResponseTime(isSuccess);
        packet.setPacketId(session.getShardingService().nextPacketId());
        packet.write(session.getSource());
    }
}
