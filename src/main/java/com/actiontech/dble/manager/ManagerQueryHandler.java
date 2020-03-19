/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.manager;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.manager.handler.*;
import com.actiontech.dble.manager.response.*;
import com.actiontech.dble.net.handler.FrontendQueryHandler;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.route.parser.ManagerParse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author mycat
 */
public class ManagerQueryHandler implements FrontendQueryHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(ManagerQueryHandler.class);
    private static final int SHIFT = 8;
    private final ManagerConnection source;

    public ManagerQueryHandler(ManagerConnection source) {
        this.source = source;
    }

    @Override
    public void setReadOnly(Boolean readOnly) {
    }

    @Override
    public void setSessionReadOnly(boolean sessionReadOnly) {
    }

    @Override
    public void query(String sql) {
        ManagerConnection c = this.source;
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(String.valueOf(c) + sql);
        }
        int rs = ManagerParse.parse(sql);
        switch (rs & 0xff) {
            case ManagerParse.SELECT:
                SelectHandler.handle(sql, c, rs >>> SHIFT);
                break;
            case ManagerParse.SET:
                c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
                break;
            case ManagerParse.SHOW:
                ShowHandler.handle(sql, c, rs >>> SHIFT);
                break;
            case ManagerParse.KILL_CONN:
                KillConnection.response(sql, rs >>> SHIFT, c);
                break;
            case ManagerParse.KILL_XA_SESSION:
                KillXASession.response(sql, rs >>> SHIFT, c);
                break;
            case ManagerParse.KILL_DDL_LOCK:
                String tableInfo = sql.substring(rs >>> SHIFT).trim();
                KillDdlLock.response(sql, tableInfo, c);
                break;
            case ManagerParse.OFFLINE:
                Offline.execute(c);
                break;
            case ManagerParse.ONLINE:
                Online.execute(c);
                break;
            case ManagerParse.PAUSE:
                PauseStart.execute(c, sql);
                break;
            case ManagerParse.RESUME:
                PauseEnd.execute(c);
                break;
            case ManagerParse.STOP:
                StopHandler.handle(sql, c, rs >>> SHIFT);
                break;
            case ManagerParse.DRY_RUN:
                DryRun.execute(c);
                break;
            case ManagerParse.RELOAD:
                ReloadHandler.handle(sql, c, rs >>> SHIFT);
                break;
            case ManagerParse.ROLLBACK:
                RollbackHandler.handle(sql, c, rs >>> SHIFT);
                break;
            case ManagerParse.CONFIGFILE:
                ConfFileHandler.handle(sql, c);
                break;
            case ManagerParse.LOGFILE:
                ShowServerLog.handle(sql, c);
                break;
            case ManagerParse.CREATE_DB:
                DatabaseHandler.handle(sql, c, true);
                break;
            case ManagerParse.DROP_DB:
                DatabaseHandler.handle(sql, c, false);
                break;
            case ManagerParse.ENABLE:
                EnableHandler.handle(sql, c, rs >>> SHIFT);
                break;
            case ManagerParse.DISABLE:
                DisableHandler.handle(sql, c, rs >>> SHIFT);
                break;
            case ManagerParse.CHECK:
                CheckHandler.handle(sql, c, rs >>> SHIFT);
                break;
            case ManagerParse.RELEASE_RELOAD_METADATA:
                ReleaseReloadMetadata.execute(c);
                break;
            case ManagerParse.DATAHOST:
                DataHostHandler.handle(sql, c);
                break;
            case ManagerParse.SPLIT:
                c.skipIdleCheck(true);
                SplitDumpHandler.handle(sql, c, rs >>> SHIFT);
                break;
            default:
                c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
        }
    }

}
