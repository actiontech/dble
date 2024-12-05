/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.route.handler;


import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.datasource.PhysicalDbInstance;
import com.oceanbase.obsharding_d.config.model.sharding.SchemaConfig;
import com.oceanbase.obsharding_d.config.model.user.SingleDbGroupUserConfig;
import com.oceanbase.obsharding_d.route.RouteResultset;
import com.oceanbase.obsharding_d.route.factory.RouteStrategyFactory;
import com.oceanbase.obsharding_d.server.parser.ServerParse;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;
import com.oceanbase.obsharding_d.services.rwsplit.RWSplitService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;

/**
 * sql hint: OBsharding-D:db_type=master/slave<br/>
 * maybe add OBsharding-D:db_type=slave_newest in feature
 *
 * @author digdeep@126.com
 */
public final class HintMasterDBHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(HintMasterDBHandler.class);

    private HintMasterDBHandler() {
    }

    public static RouteResultset route(SchemaConfig schema, String hintSQLValue, int sqlType, String realSQL, ShardingService service)
            throws SQLException {

        RouteResultset rrs = RouteStrategyFactory.getRouteStrategy().route(schema, sqlType, realSQL, service);

        LOGGER.debug("rrs(): " + rrs); // master
        Boolean isRouteToMaster;
        try {
            isRouteToMaster = isMaster(hintSQLValue, sqlType);
        } catch (UnsupportedOperationException e) {
            LOGGER.info(" sql hint 'db_type' error, ignore this hint.");
            return rrs;
        }
        LOGGER.debug("hintSQLValue:::::::::" + hintSQLValue); // slave
        rrs.setRunOnSlave(!isRouteToMaster);
        LOGGER.debug("rrs.getRunOnSlave():" + rrs.getRunOnSlave());
        return rrs;
    }

    public static PhysicalDbInstance route(String hintSQLValue, int sqlType, String realSQL, RWSplitService service) throws SQLException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("route dbInstance sql hint from " + realSQL);
        }
        boolean isRouteToMaster;
        try {
            isRouteToMaster = isMaster(hintSQLValue, sqlType);
        } catch (UnsupportedOperationException e) {
            LOGGER.warn(" sql hint 'db_type' error, ignore this hint.");
            isRouteToMaster = true;
        }

        SingleDbGroupUserConfig rwSplitUserConfig = service.getUserConfig();
        String dbGroup = rwSplitUserConfig.getDbGroup();
        PhysicalDbInstance dbInstance;
        try {
            dbInstance = OBsharding_DServer.getInstance().getConfig().getDbGroups().get(dbGroup).rwSelect(isRouteToMaster, null);
        } catch (IOException e) {
            throw new SQLNonTransientException(e);
        }
        if (null == dbInstance) {
            String msg = "can't find hint dbInstance:" + hintSQLValue;
            LOGGER.warn(msg);
            throw new SQLNonTransientException(msg);
        }
        return dbInstance;
    }

    private static Boolean isMaster(String hintSQLValue, int sqlType) {
        if (hintSQLValue != null && !hintSQLValue.trim().equals("")) {
            if (hintSQLValue.trim().equalsIgnoreCase("master")) {
                return true;
            }
            if (hintSQLValue.trim().equalsIgnoreCase("slave")) {
                if (sqlType == ServerParse.DELETE || sqlType == ServerParse.INSERT ||
                        sqlType == ServerParse.REPLACE || sqlType == ServerParse.UPDATE ||
                        sqlType == ServerParse.DDL) {
                    LOGGER.info("should not use hint 'db_type' to route 'delete', 'insert', 'replace', 'update', 'ddl' to a slave db.");
                    throw new UnsupportedOperationException();
                }
                return false;
            }
        }
        return false;
    }

}
