/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.handler;


import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.factory.RouteStrategyFactory;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Map;

/**
 * sql hint: dble:db_type=master/slave<br/>
 * maybe add dble:db_type=slave_newest in feature
 *
 * @author digdeep@126.com
 */
public class HintMasterDBHandler implements HintHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(HintMasterDBHandler.class);

    @Override
    public RouteResultset route(SchemaConfig schema, int sqlType, String realSQL, ShardingService service,
                                String hintSQLValue, int hintSqlType, Map hintMap)
            throws SQLException {

        RouteResultset rrs = RouteStrategyFactory.getRouteStrategy().route(
                schema, sqlType, realSQL, service);

        LOGGER.debug("rrs(): " + rrs); // master
        Boolean isRouteToMaster = null;

        LOGGER.debug("hintSQLValue:::::::::" + hintSQLValue); // slave

        if (hintSQLValue != null && !hintSQLValue.trim().equals("")) {
            if (hintSQLValue.trim().equalsIgnoreCase("master")) {
                isRouteToMaster = true;
            }
            if (hintSQLValue.trim().equalsIgnoreCase("slave")) {
                if (sqlType == ServerParse.DELETE || sqlType == ServerParse.INSERT ||
                        sqlType == ServerParse.REPLACE || sqlType == ServerParse.UPDATE ||
                        sqlType == ServerParse.DDL) {
                    LOGGER.info("should not use hint 'db_type' to route 'delete', 'insert', 'replace', 'update', 'ddl' to a slave db.");
                    isRouteToMaster = null;
                } else {
                    isRouteToMaster = false;
                }
            }
        }

        if (isRouteToMaster == null) {
            LOGGER.info(" sql hint 'db_type' error, ignore this hint.");
            return rrs;
        }

        if (isRouteToMaster) {
            rrs.setRunOnSlave(false);
        } else {
            rrs.setRunOnSlave(true);
        }

        LOGGER.debug("rrs.getRunOnSlave():" + rrs.getRunOnSlave());
        return rrs;
    }

}
