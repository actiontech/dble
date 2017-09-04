package com.actiontech.dble.route;

import com.actiontech.dble.cache.LayerCachePool;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.server.ServerConnection;
import com.alibaba.druid.sql.ast.SQLStatement;

import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;

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
