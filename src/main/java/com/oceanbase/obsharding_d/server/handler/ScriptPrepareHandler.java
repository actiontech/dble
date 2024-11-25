/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.server.handler;

import com.oceanbase.obsharding_d.config.ErrorCode;

import com.oceanbase.obsharding_d.server.parser.ScriptPrepareParse;
import com.oceanbase.obsharding_d.server.response.SptDrop;
import com.oceanbase.obsharding_d.server.response.SptExecute;
import com.oceanbase.obsharding_d.server.response.SptPrepare;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ScriptPrepareHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(ScriptPrepareHandler.class);

    private ScriptPrepareHandler() {
    }

    public static void handle(String stmt, ShardingService service) {
        switch (ScriptPrepareParse.parse(stmt, 0, service)) {
            case ScriptPrepareParse.PREPARE:
                SptPrepare.response(service);
                break;
            case ScriptPrepareParse.EXECUTE:
                SptExecute.response(service);
                break;
            case ScriptPrepareParse.DROP:
                SptDrop.response(service);
                break;
            default:
                LOGGER.info("You have an error in your SQL syntax:" + stmt);
                service.writeErrMessage(ErrorCode.ER_SYNTAX_ERROR, "You have an error in your SQL syntax:" + stmt);
                break;
        }
    }
}
