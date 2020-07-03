/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.route.factory.RouteStrategyFactory;

import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.statement.SQLCreateDatabaseStatement;

/**
 * Created by collapsar on 2019/07/23.
 */
public final class CreateDatabaseHandler {

    private CreateDatabaseHandler() {
    }

    public static void handle(String stmt, ShardingService service) {
        try {
            stmt = stmt.replace("/*!", "/*#");
            SQLCreateDatabaseStatement statement = (SQLCreateDatabaseStatement) RouteStrategyFactory.getRouteStrategy().parserSQL(stmt);
            String schema = statement.getName().getSimpleName();
            schema = StringUtil.removeBackQuote(schema);
            SchemaConfig sc = DbleServer.getInstance().getConfig().getSchemas().get(schema);
            if (sc != null) {
                OkPacket ok = new OkPacket();
                ok.setPacketId(1);
                ok.setAffectedRows(1);
                ok.write(service.getConnection());
            } else {
                throw new Exception("Can't create database '" + schema + "' that doesn't exists in config");
            }
        } catch (Exception e) {
            service.writeErrMessage(ErrorCode.ER_PARSE_ERROR, e.getMessage());
        }
    }
}
