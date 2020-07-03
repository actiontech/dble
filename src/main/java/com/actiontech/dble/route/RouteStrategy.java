/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route;

import com.actiontech.dble.config.model.sharding.SchemaConfig;

import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.alibaba.druid.sql.ast.SQLStatement;

import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;

/**
 * RouteStrategy
 *
 * @author wang.dw
 */
public interface RouteStrategy {
    SQLStatement parserSQL(String originSql) throws SQLSyntaxErrorException;

    RouteResultset route(SchemaConfig schema, int sqlType, String origSQL, ShardingService service)
            throws SQLException;

    RouteResultset route(SchemaConfig schema, int sqlType, String origSQL, ShardingService service, boolean isExplain)
            throws SQLException;
}
