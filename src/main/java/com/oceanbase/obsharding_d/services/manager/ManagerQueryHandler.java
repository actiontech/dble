/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.services.manager;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.net.connection.FrontendConnection;
import com.oceanbase.obsharding_d.net.mysql.OkPacket;
import com.oceanbase.obsharding_d.plan.common.exception.MySQLOutPutException;
import com.oceanbase.obsharding_d.route.parser.ManagerParse;
import com.oceanbase.obsharding_d.services.manager.handler.*;
import com.oceanbase.obsharding_d.services.manager.response.*;
import com.oceanbase.obsharding_d.singleton.TraceManager;
import com.oceanbase.obsharding_d.util.exception.DirectPrintException;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author mycat
 */
public class ManagerQueryHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(ManagerQueryHandler.class);
    private static final int SHIFT = 8;
    private final ManagerService service;

    private Boolean readOnly = true;

    public ManagerQueryHandler(ManagerService service) {
        this.service = service;
    }

    public void query(String sql) {

        service.getClusterDelayService().markDoingOrDelay(false);
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(service, "manager-query-handle");
        TraceManager.log(ImmutableMap.of("sql", sql), traceObject);
        try {
            this.service.setExecuteSql(sql);
            int rs = ManagerParse.parse(sql);
            int sqlType = rs & 0xff;
            if (sqlType > ManagerParse.MAX_READ_SEQUENCE) {
                if (readOnly) {
                    service.writeErrMessage(ErrorCode.ER_USER_READ_ONLY, "User READ ONLY");
                    return;
                }
                FrontendConnection con = service.getConnection();
                LOGGER.info("execute manager cmd from frontendConnection [{}]: {} ", con.getSimple(), sql);
            }
            switch (sqlType) {
                case ManagerParse.SELECT:
                    SelectHandler.handle(sql, service, rs >>> SHIFT);
                    break;
                case ManagerParse.SET:
                    OkPacket ok = new OkPacket();
                    ok.setPacketId(service.nextPacketId());
                    ok.write(service.getConnection());
                    break;
                case ManagerParse.SHOW:
                    ShowHandler.handle(sql, service, rs >>> SHIFT);
                    break;
                case ManagerParse.DESCRIBE:
                    Describe.execute(sql, service);
                    break;
                case ManagerParse.KILL_CONN:
                    KillConnection.response(sql, rs >>> SHIFT, service);
                    break;
                case ManagerParse.KILL_XA_SESSION:
                    KillXASession.response(sql, rs >>> SHIFT, service);
                    break;
                case ManagerParse.KILL_DDL_LOCK:
                    String tableInfo = sql.substring(rs >>> SHIFT).trim();
                    KillDdlLock.response(sql, tableInfo, service);
                    break;
                case ManagerParse.KILL_LOAD_DATA:
                    KillLoadData.response(service);
                    break;
                case ManagerParse.KILL_CLUSTER_RENEW_THREAD:
                    KillClusterRenewThread.response(service, sql.substring(rs >>> SHIFT).trim());
                    break;
                case ManagerParse.OFFLINE:
                    Offline.execute(service);
                    break;
                case ManagerParse.ONLINE:
                    Online.execute(service);
                    break;
                case ManagerParse.PAUSE:
                    PauseStart.execute(service, sql, rs >>> SHIFT);
                    break;
                case ManagerParse.RESUME:
                    PauseEnd.execute(service);
                    break;
                case ManagerParse.STOP:
                    StopHandler.handle(sql, service, rs >>> SHIFT);
                    break;
                case ManagerParse.START:
                    StartHandler.handle(sql, service, rs >>> SHIFT);
                    break;
                case ManagerParse.DRY_RUN:
                    DryRun.execute(service);
                    break;
                case ManagerParse.RELOAD:
                    ReloadHandler.handle(sql, service, rs >>> SHIFT);
                    break;
                case ManagerParse.CREATE_DB:
                    DatabaseHandler.handle(sql, service, true, rs >>> SHIFT);
                    break;
                case ManagerParse.DROP_DB:
                    DatabaseHandler.handle(sql, service, false, rs >>> SHIFT);
                    break;
                case ManagerParse.DROP_STATISTIC_QUEUE_USAGE:
                    StatisticCf.Queue.drop(service);
                    break;
                case ManagerParse.ENABLE:
                    EnableHandler.handle(sql, service, rs >>> SHIFT);
                    break;
                case ManagerParse.DISABLE:
                    DisableHandler.handle(sql, service, rs >>> SHIFT);
                    break;
                case ManagerParse.CHECK:
                    CheckHandler.handle(sql, service, rs >>> SHIFT);
                    break;
                case ManagerParse.RELEASE_RELOAD_METADATA:
                    ReleaseReloadMetadata.execute(service);
                    break;
                case ManagerParse.DB_GROUP:
                    DbGroupHAHandler.handle(sql, service, rs >> SHIFT);
                    break;
                case ManagerParse.SPLIT:
                    service.getConnection().setSkipCheck(true);
                    service.getClusterDelayService().markDoingOrDelay(true);
                    SplitDumpHandler.handle(sql, service, rs >>> SHIFT);
                    break;
                case ManagerParse.SPLIT_LOAD_DATA:
                    service.getConnection().setSkipCheck(true);
                    SplitLoadDataHandler.handle(sql, service, rs >>> SHIFT);
                    break;
                case ManagerParse.FLOW_CONTROL:
                    FlowControlHandler.handle(sql, service, rs >>> SHIFT);
                    break;
                case ManagerParse.INSERT:
                    service.getClusterDelayService().markDoingOrDelay(true);
                    OBsharding_DServer.getInstance().getComplexQueryExecutor().execute(() -> {
                        (new InsertHandler()).handle(sql, service);
                    });
                    break;
                case ManagerParse.DELETE:
                    service.getClusterDelayService().markDoingOrDelay(true);
                    OBsharding_DServer.getInstance().getComplexQueryExecutor().execute(() -> {
                        (new DeleteHandler()).handle(sql, service);
                    });
                    break;
                case ManagerParse.UPDATE:
                    service.getClusterDelayService().markDoingOrDelay(true);
                    OBsharding_DServer.getInstance().getComplexQueryExecutor().execute(() -> {
                        (new UpdateHandler()).handle(sql, service);
                    });
                    break;
                case ManagerParse.FRESH_CONN:
                    FreshBackendConnHandler.handle(sql, service, rs >>> SHIFT);
                    break;
                case ManagerParse.USE:
                    UseHandler.handle(sql, service, rs >>> SHIFT);
                    break;
                case ManagerParse.TRUNCATE_TABLE:
                    TruncateHander.handle(sql, service);
                    break;
                case ManagerParse.CLUSTER:
                    ClusterManageHandler.handle(sql, service, rs >>> SHIFT);
                    break;
                default:
                    service.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
            }
        } catch (DirectPrintException e) {
            service.writeErrMessage(ErrorCode.ER_YES, e.getMessage());
        } catch (MySQLOutPutException e) {
            service.writeErrMessage(e.getSqlState(), e.getMessage(), e.getErrorCode());
            LOGGER.warn("unknown error:", e);
        } catch (Exception e) {
            service.writeErrMessage(ErrorCode.ER_YES, "get error call manager command: " + e.getMessage());
            LOGGER.warn("unknown error:", e);
        } finally {
            service.getConnection().setSkipCheck(false);
        }
    }

    public void setReadOnly(Boolean readOnly) {
        this.readOnly = readOnly;
    }

}
