package io.mycat.route.handler;

import io.mycat.MycatServer;
import io.mycat.backend.datasource.PhysicalDBNode;
import io.mycat.cache.LayerCachePool;
import io.mycat.config.model.SchemaConfig;
import io.mycat.route.RouteResultset;
import io.mycat.route.util.RouterUtil;
import io.mycat.server.ServerConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLNonTransientException;
import java.util.Map;

/**
 * 处理注释中类型为datanode 的情况
 *
 * @author zhuam
 */
public class HintDataNodeHandler implements HintHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(HintDataNodeHandler.class);

    @Override
    public RouteResultset route(SchemaConfig schema, int sqlType, String realSQL,
                                String charset, ServerConnection sc, LayerCachePool cachePool, String hintSQLValue, int hintSqlType, Map hintMap)
            throws SQLNonTransientException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("route datanode sql hint from " + realSQL);
        }

        RouteResultset rrs = new RouteResultset(realSQL, sqlType);
        PhysicalDBNode dataNode = MycatServer.getInstance().getConfig().getDataNodes().get(hintSQLValue);
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
