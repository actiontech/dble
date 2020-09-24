/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.server.response;

import com.actiontech.dble.config.ErrorCode;

import com.actiontech.dble.services.mysqlsharding.ShardingService;

import java.util.List;

public final class SptExecute {
    private SptExecute() {
    }

    public static String queryUserVar(String name, ShardingService service) {
        String key = "@" + name;
        return service.getUsrVariables().get(key);
    }

    public static void response(ShardingService service) {
        String name = service.getSptPrepare().getName();
        List<String> parts = service.getSptPrepare().getPrepare(name);
        if (parts == null) {
            service.writeErrMessage(ErrorCode.ER_UNKNOWN_STMT_HANDLER, "Unknown prepared statement handler" + name + " given to EXECUTE");
            return;
        }

        int argNum = 0;
        List<String> args = service.getSptPrepare().getArguments();
        if (args != null) {
            argNum = args.size();
        }
        service.getSptPrepare().setArguments(null);

        if (parts.size() != argNum + 1) {
            service.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "Incorrect arguments to EXECUTE");
            return;
        }

        StringBuilder stmt = new StringBuilder(parts.get(0));
        if (args != null) {
            for (int i = 1; i < parts.size(); i++) {
                String val = queryUserVar(args.get(i - 1), service);
                if (val == null) {
                    service.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "Incorrect arguments to EXECUTE");
                    return;
                }
                stmt.append(val);
                stmt.append(parts.get(i));
            }
        }
        service.query(stmt.toString());
    }
}
