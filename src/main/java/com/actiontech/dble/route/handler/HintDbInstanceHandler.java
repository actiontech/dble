/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.user.RwSplitUserConfig;
import com.actiontech.dble.config.model.user.UserConfig;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.util.StringUtil;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLNonTransientException;
import java.util.Map;

/**
 * HintDbInstanceHandler
 *
 * @author AMGuo
 */
public class HintDbInstanceHandler implements HintHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(HintDbInstanceHandler.class);

    @Override
    public RouteResultset route(SchemaConfig schema, int sqlType, String realSQL, ShardingService service,
                                String hintSQLValue, int hintSqlType, Map hintMap)
            throws SQLNonTransientException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("route dbInstance sql hint from " + realSQL);
        }
        UserConfig userConfig = (UserConfig) service.getUserConfig();
        if (!(userConfig instanceof RwSplitUserConfig)) {
            String msg = "Unsupported " + new Gson().toJson(hintMap.values()) + " for userType:" + userConfig.getClass().getSimpleName() + " username:" + userConfig.getName();
            LOGGER.info(msg);
            throw new SQLNonTransientException(msg);
        }

        hintSQLValue = hintSQLValue.trim();
        RouteResultset rrs = new RouteResultset(realSQL, hintSqlType);
        boolean isExist = existDbInstance(hintSQLValue);
        if (isExist) {
            rrs = RouterUtil.routeToSingleDbInstance(rrs, hintSQLValue);
        } else {
            String msg = "can't find hint dbInstance:" + hintSQLValue;
            LOGGER.info(msg);
            throw new SQLNonTransientException(msg);
        }

        return rrs;
    }

    private boolean existDbInstance(String hintSQLValue) {
        if (StringUtil.isEmpty(hintSQLValue)) {
            return false;
        }
        Map<String, PhysicalDbGroup> dbGroupMap = DbleServer.getInstance().getConfig().getDbGroups();
        for (Map.Entry<String, PhysicalDbGroup> dbGroupEntry : dbGroupMap.entrySet()) {
            boolean isExist = dbGroupEntry.getValue()
                    .getAllActiveDbInstances().stream()
                    .anyMatch(dbInstance -> StringUtil.equals(dbInstance.getConfig().getUrl().trim(), hintSQLValue.trim()));
            if (isExist) {
                return true;
            }
        }
        return false;
    }

}
