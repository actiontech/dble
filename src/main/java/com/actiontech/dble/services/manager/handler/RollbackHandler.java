/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.services.manager.handler;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.services.manager.response.RollbackConfig;
import com.actiontech.dble.route.parser.ManagerParseRollback;

/**
 * @author mycat
 */
public final class RollbackHandler {

    private RollbackHandler() {
    }

    public static void handle(String stmt, ManagerService service, int offset) {
        switch (ManagerParseRollback.parse(stmt, offset)) {
            case ManagerParseRollback.CONFIG:
                RollbackConfig.execute(service);
                break;
            default:
                service.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
        }
    }

}
