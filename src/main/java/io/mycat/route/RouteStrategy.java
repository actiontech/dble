package io.mycat.route;

import com.alibaba.druid.sql.ast.SQLStatement;
import io.mycat.cache.LayerCachePool;
import io.mycat.config.model.SchemaConfig;
import io.mycat.server.ServerConnection;

import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;

/**
 * 路由策略接口
 *
 * @author wang.dw
 */
public interface RouteStrategy {
    public SQLStatement parserSQL(String originSql) throws SQLSyntaxErrorException;

    RouteResultset route(SchemaConfig schema, int sqlType, String origSQL, String charset, ServerConnection sc, LayerCachePool cachePool)
            throws SQLException;
}
