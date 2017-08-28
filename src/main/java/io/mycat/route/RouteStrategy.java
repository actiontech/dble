package io.mycat.route;

import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;

import com.alibaba.druid.sql.ast.SQLStatement;

import io.mycat.cache.LayerCachePool;
import io.mycat.config.model.SchemaConfig;
import io.mycat.server.ServerConnection;

/**
 * RouteStrategy
 *
 * @author wang.dw
 */
public interface RouteStrategy {
    SQLStatement parserSQL(String originSql) throws SQLSyntaxErrorException;

    RouteResultset route(SchemaConfig schema, int sqlType, String origSQL, String charset, ServerConnection sc, LayerCachePool cachePool)
            throws SQLException;
}
