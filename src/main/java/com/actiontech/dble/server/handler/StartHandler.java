/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server.handler;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.server.parser.ServerParseStart;

/**
 * @author mycat
 */
public final class StartHandler {
    private StartHandler() {
    }

    public static void handle(String stmt, ServerConnection c, int offset) {
        switch (ServerParseStart.parse(stmt, offset)) {
            case ServerParseStart.TRANSACTION:
                BeginHandler.handle(stmt, c);
                break;
            case ServerParseStart.READCHARCS:
                c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
                break;
            default:
                c.execute(stmt, ServerParse.START);
        }
    }

}
