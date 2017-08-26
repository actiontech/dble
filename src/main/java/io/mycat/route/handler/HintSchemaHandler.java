package io.mycat.route.handler;

import io.mycat.MycatServer;
import io.mycat.cache.LayerCachePool;
import io.mycat.config.model.SchemaConfig;
import io.mycat.route.RouteResultset;
import io.mycat.route.RouteStrategy;
import io.mycat.route.factory.RouteStrategyFactory;
import io.mycat.server.ServerConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.Map;

/**
 * HintSchemaHandler
 */
public class HintSchemaHandler implements HintHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(HintSchemaHandler.class);

    private RouteStrategy routeStrategy;

    public HintSchemaHandler() {
        this.routeStrategy = RouteStrategyFactory.getRouteStrategy();
    }

    /**
     *
     * @param schema
     * @param sqlType
     * @param realSQL
     * @param charset
     * @param sc
     * @param cachePool
     * @param hintSQLValue
     * @return
     * @throws SQLNonTransientException
     */
    @Override
    public RouteResultset route(SchemaConfig schema,
                                int sqlType, String realSQL, String charset, ServerConnection sc,
                                LayerCachePool cachePool, String hintSQLValue, int hintSqlType, Map hintMap)
            throws SQLException {
        SchemaConfig tempSchema = MycatServer.getInstance().getConfig().getSchemas().get(hintSQLValue);
        if (tempSchema != null) {
            return routeStrategy.route(tempSchema, sqlType, realSQL, charset, sc, cachePool);
        } else {
            String msg = "can't find hint schema:" + hintSQLValue;
            LOGGER.warn(msg);
            throw new SQLNonTransientException(msg);
        }
    }
}
