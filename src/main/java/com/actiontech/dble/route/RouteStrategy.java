/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route;

import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.services.mysqlsharding.ShardingService;

import java.sql.SQLException;

/**
 * RouteStrategy
 *
 * @author wang.dw
 */
public interface RouteStrategy {

    RouteResultset route(SchemaConfig schema, int sqlType, String origSQL, ShardingService service)
            throws SQLException;

    RouteResultset route(SchemaConfig schema, int sqlType, String origSQL, ShardingService service, boolean isExplain)
            throws SQLException;
}
