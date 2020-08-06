/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.handler;

import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.route.RouteResultset;

import com.actiontech.dble.services.mysqlsharding.ShardingService;

import java.sql.SQLException;
import java.util.Map;

/**
 * router according to  the hint
 */
public interface HintHandler {

    RouteResultset route(SchemaConfig schema,
                         int sqlType, String realSQL, ShardingService service,
                         String hintSQLValue, int hintSqlType, Map hintMap)
            throws SQLException;
}
