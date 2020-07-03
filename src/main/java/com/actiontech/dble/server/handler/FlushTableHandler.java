/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.handler;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.route.factory.RouteStrategyFactory;

import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlFlushStatement;

/**
 * Created by collapsar on 2019/07/23.
 */
public final class FlushTableHandler {

    private FlushTableHandler() {
    }

    public static void handle(String stmt, ShardingService service) {
        try {
            stmt = stmt.replace("/*!", "/*#");
            MySqlFlushStatement statement = (MySqlFlushStatement) RouteStrategyFactory.getRouteStrategy().parserSQL(stmt);
            OkPacket ok = new OkPacket();
            ok.setPacketId(1);
            ok.setAffectedRows(1);
            ok.write(service.getConnection());
        } catch (Exception e) {
            service.writeErrMessage(ErrorCode.ER_PARSE_ERROR, e.getMessage());
        }
    }
}
