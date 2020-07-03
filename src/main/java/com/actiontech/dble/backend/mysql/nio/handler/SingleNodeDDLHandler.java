package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.cluster.values.DDLTraceInfo;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.singleton.DDLTraceManager;
import com.actiontech.dble.util.StringUtil;

/**
 * Created by szf on 2019/12/3.
 */
public class SingleNodeDDLHandler extends SingleNodeHandler {

    public SingleNodeDDLHandler(RouteResultset rrs, NonBlockingSession session) {
        super(rrs, session);
    }

    public void execute() throws Exception {
        DDLTraceManager.getInstance().updateDDLStatus(DDLTraceInfo.DDLStage.EXECUTE_START, session.getShardingService());
        try {
            super.execute();
        } catch (Exception e) {
            DDLTraceManager.getInstance().endDDL(session.getShardingService(), e.getMessage());
            throw e;
        }
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
    public void errorResponse(byte[] data, AbstractService service) {
        DDLTraceManager.getInstance().updateConnectionStatus(session.getShardingService(),
                (MySQLResponseService) service, DDLTraceInfo.DDLConnectionStatus.CONN_EXECUTE_ERROR);
        DDLTraceManager.getInstance().endDDL(session.getShardingService(), "ddl end with execution failure");
        super.errorResponse(data, service);
    }

    @Override
    public void connectionClose(AbstractService service, String reason) {
        DDLTraceManager.getInstance().updateConnectionStatus(session.getShardingService(),
                (MySQLResponseService) service, DDLTraceInfo.DDLConnectionStatus.EXECUTE_CONN_CLOSE);
        DDLTraceManager.getInstance().endDDL(session.getShardingService(), reason);
        super.connectionClose(service, reason);
    }


    @Override
    public void okResponse(byte[] data, AbstractService service) {
        boolean executeResponse = ((MySQLResponseService) service).syncAndExecute();
        if (executeResponse) {
            DDLTraceManager.getInstance().updateConnectionStatus(session.getShardingService(), (MySQLResponseService) service, DDLTraceInfo.DDLConnectionStatus.CONN_EXECUTE_SUCCESS);
            // handleSpecial
            boolean metaInitial = session.handleSpecial(rrs, true, null);
            if (!metaInitial) {
                DDLTraceManager.getInstance().endDDL(session.getShardingService(), "ddl end with meta failure");
                executeMetaDataFailed((MySQLResponseService) service);
            } else {
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
                session.setResponseTime(true);
                session.multiStatementPacket(ok);
                if (writeToClient.compareAndSet(false, true)) {
                    ok.write(sessionShardingService.getConnection());
                }
            }
        }
    }

    private void executeMetaDataFailed(MySQLResponseService service) {
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.setPacketId(session.getShardingService().nextPacketId());
        errPacket.setErrNo(ErrorCode.ER_META_DATA);
        String errMsg = "Create TABLE OK, but generate metedata failed. The reason may be that the current druid parser can not recognize part of the sql" +
                " or the user for backend mysql does not have permission to execute the heartbeat sql.";
        errPacket.setMessage(StringUtil.encode(errMsg, session.getShardingService().getCharset().getResults()));

        session.setBackendResponseEndTime(service);
        session.releaseConnectionIfSafe(service, false);
        session.setResponseTime(false);
        session.multiStatementPacket(errPacket);
        doSqlStat();
        if (writeToClient.compareAndSet(false, true)) {
            errPacket.write(session.getSource());
        }
    }

    @Override
    protected void backConnectionErr(ErrorPacket errPkg, MySQLResponseService service, boolean syncFinished) {
        ShardingService sessionShardingService = session.getShardingService();
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
        String errMsg = " errNo:" + errPkg.getErrNo() + " " + new String(errPkg.getMessage());
        sessionShardingService.setTxInterrupt(errMsg);
        if (writeToClient.compareAndSet(false, true)) {
            session.handleSpecial(rrs, false, null);
            errPkg.write(sessionShardingService.getConnection());
        }
    }

}
