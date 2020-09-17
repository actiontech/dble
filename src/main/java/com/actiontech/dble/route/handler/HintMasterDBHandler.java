/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.handler;


import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.user.RwSplitUserConfig;
import com.actiontech.dble.config.model.user.UserConfig;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.factory.RouteStrategyFactory;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.services.rwsplit.RWSplitService;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.Map;
import java.util.Optional;

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

    @Override
    public PhysicalDbInstance routeRwSplit(int sqlType, String realSQL, RWSplitService service, String hintSQLValue, int hintSqlType, Map hintMap) throws SQLException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("route dbInstance sql hint from " + realSQL);
        }
        UserConfig userConfig = service.getUserConfig();
        if (!(userConfig instanceof RwSplitUserConfig)) {
            String msg = "Unsupported " + new Gson().toJson(hintMap.values()) + " for userType:" + userConfig.getClass().getSimpleName() + " username:" + userConfig.getName();
            LOGGER.info(msg);
            throw new SQLNonTransientException(msg);
        }
        Boolean isRouteToMaster;
        try {
            isRouteToMaster = isMaster(hintSQLValue, sqlType);
        } catch (UnsupportedOperationException e) {
            LOGGER.info(" sql hint 'db_type' error, ignore this hint.");
            isRouteToMaster = true;
        }

        RwSplitUserConfig rwSplitUserConfig = (RwSplitUserConfig) service.getUserConfig();
        hintSQLValue = hintSQLValue.trim();
        PhysicalDbInstance dbInstance = findMasterDbInstance(rwSplitUserConfig, isRouteToMaster);
        if (null == dbInstance) {
            String msg = "can't find hint dbInstance:" + hintSQLValue;
            LOGGER.info(msg);
            throw new SQLNonTransientException(msg);
        }
        return dbInstance;
    }

    private Boolean isMaster(String hintSQLValue, int sqlType) {
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


    private PhysicalDbInstance findMasterDbInstance(RwSplitUserConfig userConfig, boolean isMaster) {
        PhysicalDbGroup dbGroupMap = DbleServer.getInstance().getConfig().getDbGroups().get(userConfig.getDbGroup());
        Optional<PhysicalDbInstance> dbInstanceOptional = dbGroupMap.
                getDbInstances(true).stream().
                filter(dbInstance -> dbInstance.getConfig().isPrimary() == isMaster).
                findFirst();
        return dbInstanceOptional.orElse(null);
    }
}
