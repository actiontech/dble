/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.server.response;

import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.server.parser.ScriptPrepareParse;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;

import java.util.LinkedList;
import java.util.List;

public final class SptPrepare {
    private SptPrepare() {
    }

    private static boolean checksync(String stmt, ShardingService service) {
        return true;
    }

    public static void response(ShardingService service) {
        String stmt = service.getSptPrepare().getExePrepare();
        if (stmt == null) {
            service.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "SQL syntax error");
            return;
        }

        if (checksync(stmt, service)) {
            String name = service.getSptPrepare().getName();
            List<String> args = new LinkedList();
            ScriptPrepareParse.parseStmt(stmt, args);
            service.getSptPrepare().setPrepare(name, args);
            service.writeOkPacket();
        } else {
            service.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "SQL syntax error");
        }
    }
}
