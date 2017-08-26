package io.mycat.route.handler;


import io.mycat.cache.LayerCachePool;
import io.mycat.config.model.SchemaConfig;
import io.mycat.route.RouteResultset;
import io.mycat.route.factory.RouteStrategyFactory;
import io.mycat.server.ServerConnection;
import io.mycat.server.parser.ServerParse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Map;

/**
 *  sql hint: mycat:db_type=master/slave<br/>
 *  maybe add mycat:db_type=slave_newest in feature
 *
 * @author digdeep@126.com
 */
public class HintMasterDBHandler implements HintHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(HintMasterDBHandler.class);

    @Override
    public RouteResultset route(SchemaConfig schema, int sqlType,
                                String realSQL, String charset,
                                ServerConnection sc, LayerCachePool cachePool, String hintSQLValue, int hintSqlType, Map hintMap)
            throws SQLException {

        RouteResultset rrs = RouteStrategyFactory.getRouteStrategy().route(
                schema, sqlType, realSQL, charset, sc, cachePool);

        LOGGER.debug("schema.rrs(): " + rrs); // master
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
                    LOGGER.error("should not use hint 'db_type' to route 'delete', 'insert', 'replace', 'update', 'ddl' to a slave db.");
                    isRouteToMaster = null;
                } else {
                    isRouteToMaster = false;
                }
            }
        }

        if (isRouteToMaster == null) {
            LOGGER.warn(" sql hint 'db_type' error, ignore this hint.");
            return rrs;
        }

        if (isRouteToMaster) {
            rrs.setRunOnSlave(false);
        }

        if (!isRouteToMaster) {
            rrs.setRunOnSlave(true);
        }

        LOGGER.debug("rrs.getRunOnSlave():" + rrs.getRunOnSlave());
        return rrs;
    }

}
