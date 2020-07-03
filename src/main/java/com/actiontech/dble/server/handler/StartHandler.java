/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server.handler;

import com.actiontech.dble.config.ErrorCode;

import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.server.parser.ServerParseStart;
import com.actiontech.dble.services.mysqlsharding.ShardingService;

/**
 * @author mycat
 */
public final class StartHandler {
    private StartHandler() {
    }

    public static void handle(String stmt, ShardingService service, int offset) {
        switch (ServerParseStart.parse(stmt, offset)) {
            case ServerParseStart.TRANSACTION:
                BeginHandler.handle(stmt, service);
                break;
            case ServerParseStart.READCHARCS:
                service.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
                break;
            default:
                service.execute(stmt, ServerParse.START);
        }
    }

}
