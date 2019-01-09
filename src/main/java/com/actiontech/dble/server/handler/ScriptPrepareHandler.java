/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server.handler;

import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.parser.ScriptPrepareParse;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.server.response.SptPrepare;
import com.actiontech.dble.server.response.SptExecute;
import com.actiontech.dble.server.response.SptDrop;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ScriptPrepareHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(ScriptPrepareHandler.class);
    private ScriptPrepareHandler() {
    }

    public static void handle(String stmt, ServerConnection c) {
        switch (ScriptPrepareParse.parse(stmt, 0, c)) {
            case ScriptPrepareParse.PREPARE:
                SptPrepare.response(c);
                break;
            case ScriptPrepareParse.EXECUTE:
                SptExecute.response(c);
                break;
            case ScriptPrepareParse.DROP:
                SptDrop.response(c);
                break;
            default:
                LOGGER.info("You have an error in your SQL syntax:" + stmt);
                c.writeErrMessage(ErrorCode.ER_SYNTAX_ERROR, "You have an error in your SQL syntax:" + stmt);
                break;
        }
    }
}
