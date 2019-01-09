/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server.response;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.server.ServerConnection;

import java.util.List;

public final class SptExecute {
    private SptExecute() {
    }

    public static String queryUserVar(String name, ServerConnection c) {
        String key = "@" + name;
        return c.getUsrVariables().get(key);
    }

    public static void response(ServerConnection c) {
        String name = c.getSptPrepare().getName();
        List<String> parts = c.getSptPrepare().getPrepare(name);
        if (parts == null) {
            c.writeErrMessage(ErrorCode.ER_UNKNOWN_STMT_HANDLER, "Unknown prepared statement handler" + name + " given to EXECUTE");
            return;
        }

        int argnum = 0;
        List<String> args = c.getSptPrepare().getArguments();
        if (args != null) {
            argnum = args.size();
        }
        c.getSptPrepare().setArguments(null);

        if (parts.size() != argnum + 1) {
            c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "Incorrect arguments to EXECUTE");
            return;
        }

        StringBuilder stmt = new StringBuilder(parts.get(0));
        for (int i = 1; i < parts.size() ; i++) {
            String val = queryUserVar(args.get(i - 1), c);
            if (val == null) {
                c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "Incorrect arguments to EXECUTE");
                return;
            }
            stmt.append(val);
            stmt.append(parts.get(i));
        }
        c.query(stmt.toString());
    }
}
