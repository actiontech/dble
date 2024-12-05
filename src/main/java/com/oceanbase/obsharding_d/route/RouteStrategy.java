/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.route;

import com.oceanbase.obsharding_d.config.model.sharding.SchemaConfig;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;

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
