/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDBNode;
import com.actiontech.dble.cache.LayerCachePool;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.server.ServerConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLNonTransientException;
import java.util.Map;

/**
 * HintDataNodeHandler
 *
 * @author zhuam
 */
public class HintDataNodeHandler implements HintHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(HintDataNodeHandler.class);

    @Override
    public RouteResultset route(SchemaConfig schema, int sqlType, String realSQL, ServerConnection sc,
                                LayerCachePool cachePool, String hintSQLValue, int hintSqlType, Map hintMap)
            throws SQLNonTransientException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("route datanode sql hint from " + realSQL);
        }

        RouteResultset rrs = new RouteResultset(realSQL, sqlType);
        PhysicalDBNode dataNode = DbleServer.getInstance().getConfig().getDataNodes().get(hintSQLValue);
        if (dataNode != null) {
            rrs = RouterUtil.routeToSingleNode(rrs, dataNode.getName());
        } else {
            String msg = "can't find hint datanode:" + hintSQLValue;
            LOGGER.warn(msg);
            throw new SQLNonTransientException(msg);
        }

        return rrs;
    }

}
