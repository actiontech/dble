/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese
 * opensource volunteers. you can redistribute it and/or modify it under the
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Any questions about this component can be directed to it's project Web address
 * https://code.google.com/p/opencloudb/.
 *
 */
package com.actiontech.dble.manager;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.manager.handler.*;
import com.actiontech.dble.manager.response.KillConnection;
import com.actiontech.dble.manager.response.Offline;
import com.actiontech.dble.manager.response.Online;
import com.actiontech.dble.net.handler.FrontendQueryHandler;
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

    public void setReadOnly(Boolean readOnly) {
    }

    @Override
    public void query(String sql) {
        ManagerConnection c = this.source;
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(String.valueOf(c) + sql);
        }
        int rs = ManagerParse.parse(sql);
        switch (rs & 0xff) {
            case ManagerParse.SHOW:
                ShowHandler.handle(sql, c, rs >>> SHIFT);
                break;
            case ManagerParse.SWITCH:
                SwitchHandler.handler(sql, c, rs >>> SHIFT);
                break;
            case ManagerParse.KILL_CONN:
                KillConnection.response(sql, rs >>> SHIFT, c);
                break;
            case ManagerParse.OFFLINE:
                Offline.execute(c);
                break;
            case ManagerParse.ONLINE:
                Online.execute(c);
                break;
            case ManagerParse.STOP:
                StopHandler.handle(sql, c, rs >>> SHIFT);
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
            default:
                c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
        }
    }

}
