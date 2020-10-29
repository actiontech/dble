/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.services.manager;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.route.parser.ManagerParse;
import com.actiontech.dble.services.manager.handler.*;
import com.actiontech.dble.services.manager.response.*;
import com.actiontech.dble.singleton.TraceManager;
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
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(String.valueOf(service) + sql);
        }
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(service, "manager-query-handle");
        TraceManager.log(ImmutableMap.of("sql", sql), traceObject);
        try {
            int rs = ManagerParse.parse(sql);
            int sqlType = rs & 0xff;
            if (readOnly && sqlType != ManagerParse.SELECT && sqlType != ManagerParse.SHOW) {
                service.writeErrMessage(ErrorCode.ER_USER_READ_ONLY, "User READ ONLY");
                return;
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
                case ManagerParse.OFFLINE:
                    Offline.execute(service);
                    break;
                case ManagerParse.ONLINE:
                    Online.execute(service);
                    break;
                case ManagerParse.PAUSE:
                    PauseStart.execute(service, sql);
                    break;
                case ManagerParse.RESUME:
                    PauseEnd.execute(service);
                    break;
                case ManagerParse.STOP:
                    StopHandler.handle(sql, service, rs >>> SHIFT);
                    break;
                case ManagerParse.DRY_RUN:
                    DryRun.execute(service);
                    break;
                case ManagerParse.RELOAD:
                    ReloadHandler.handle(sql, service, rs >>> SHIFT);
                    break;
                case ManagerParse.CONFIGFILE:
                    ConfFileHandler.handle(sql, service);
                    break;
                case ManagerParse.LOGFILE:
                    ShowServerLog.handle(sql, service);
                    break;
                case ManagerParse.CREATE_DB:
                    DatabaseHandler.handle(sql, service, true);
                    break;
                case ManagerParse.DROP_DB:
                    DatabaseHandler.handle(sql, service, false);
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
                    DbGroupHAHandler.handle(sql, service);
                    break;
                case ManagerParse.SPLIT:
                    SplitDumpHandler.handle(sql, service, rs >>> SHIFT);
                    break;
                case ManagerParse.FLOW_CONTROL:
                    FlowControlHandler.handle(sql, service);
                    break;
                case ManagerParse.INSERT:
                    DbleServer.getInstance().getComplexQueryExecutor().execute(() -> {
                        (new InsertHandler()).handle(sql, service);
                    });
                    break;
                case ManagerParse.DELETE:
                    DbleServer.getInstance().getComplexQueryExecutor().execute(() -> {
                        (new DeleteHandler()).handle(sql, service);
                    });
                    break;
                case ManagerParse.UPDATE:
                    DbleServer.getInstance().getComplexQueryExecutor().execute(() -> {
                        (new UpdateHandler()).handle(sql, service);
                    });
                    break;
                case ManagerParse.FRESH_CONN:
                    FreshBackendConnHandler.handle(sql, service);
                    break;
                case ManagerParse.USE:
                    UseHandler.handle(sql, service, rs >>> SHIFT);
                    break;
                default:
                    service.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
            }
        } catch (Exception e) {
            service.writeErrMessage(ErrorCode.ER_YES, "get error call manager command " + e.getMessage());
            LOGGER.warn("unknown error:", e);
        }
    }

    public void setReadOnly(Boolean readOnly) {
        this.readOnly = readOnly;
    }

}
