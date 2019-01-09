/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server.response;

import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.parser.ScriptPrepareParse;

import java.util.List;
import java.util.LinkedList;

public final class SptPrepare {
    private SptPrepare() {
    }

    private static boolean checksync(String stmt, ServerConnection c) {
        return true;
    }

    public static void response(ServerConnection c) {
        String stmt = c.getSptPrepare().getExePrepare();
        if (stmt == null) {
            c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "SQL syntax error");
            return;
        }

        if (checksync(stmt, c)) {
            String name = c.getSptPrepare().getName();
            List<String> args = new LinkedList<String>();
            ScriptPrepareParse.parseStmt(stmt, args);
            c.getSptPrepare().setPrepare(name, args);
            c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
        } else {
            c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "SQL syntax error");
            return;
        }
    }
}
