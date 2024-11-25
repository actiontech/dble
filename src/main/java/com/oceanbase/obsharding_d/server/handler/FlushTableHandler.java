/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.server.handler;

import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.net.mysql.OkPacket;
import com.oceanbase.obsharding_d.route.parser.util.DruidUtil;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;

/**
 * Created by collapsar on 2019/07/23.
 */
public final class FlushTableHandler {

    private FlushTableHandler() {
    }

    public static void handle(String stmt, ShardingService service) {
        try {
            stmt = stmt.replace("/*!", "/*#");
            DruidUtil.parseMultiSQL(stmt);
            OkPacket ok = new OkPacket();
            ok.setPacketId(1);
            ok.setAffectedRows(1);
            ok.write(service.getConnection());
        } catch (Exception e) {
            service.writeErrMessage(ErrorCode.ER_PARSE_ERROR, e.getMessage());
        }
    }
}
