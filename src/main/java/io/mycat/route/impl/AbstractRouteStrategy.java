package io.mycat.route.impl;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.cache.LayerCachePool;
import io.mycat.config.model.SchemaConfig;
import io.mycat.route.RouteResultset;
import io.mycat.route.RouteStrategy;
import io.mycat.route.util.RouterUtil;
import io.mycat.server.ServerConnection;
import io.mycat.server.parser.ServerParse;
import io.mycat.sqlengine.mpp.LoadData;

public abstract class AbstractRouteStrategy implements RouteStrategy {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRouteStrategy.class);

    @Override
    public RouteResultset route(SchemaConfig schema, int sqlType, String origSQL,
                                String charset, ServerConnection sc, LayerCachePool cachePool) throws SQLException {

        RouteResultset rrs = new RouteResultset(origSQL, sqlType);

        /*
         * debug mode and load data ,no cache
         */
        if (LOGGER.isDebugEnabled() && origSQL.startsWith(LoadData.LOAD_DATA_HINT)) {
            rrs.setCacheAble(false);
        }

        if (schema == null) {
            rrs = routeNormalSqlWithAST(null, origSQL, rrs, charset, cachePool, sc);
        } else {
            if (sqlType == ServerParse.SHOW) {
                rrs.setStatement(origSQL);
                rrs = RouterUtil.routeToSingleNode(rrs, schema.getRandomDataNode());
            } else {
                rrs = routeNormalSqlWithAST(schema, origSQL, rrs, charset, cachePool, sc);
            }
        }

        return rrs;
    }


    /**
     * routeNormalSqlWithAST
     */
    public abstract RouteResultset routeNormalSqlWithAST(SchemaConfig schema, String stmt, RouteResultset rrs,
                                                         String charset, LayerCachePool cachePool, ServerConnection sc) throws SQLException;


}
