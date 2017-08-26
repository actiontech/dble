package io.mycat.route.parser.druid;

import com.alibaba.druid.sql.ast.SQLStatement;
import io.mycat.cache.LayerCachePool;
import io.mycat.config.model.SchemaConfig;
import io.mycat.route.RouteResultset;
import io.mycat.server.ServerConnection;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;

/**
 * Parser SQLStatement
 *
 * @author wang.dw
 */
public interface DruidParser {
    /**
     * use MycatSchemaStatVisitor, get the info of tables,tableAliasMap,conditions and so on
     *
     * @param schema
     * @param stmt
     * @param sc
     */
    SchemaConfig parser(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, String originSql, LayerCachePool cachePool, MycatSchemaStatVisitor schemaStatVisitor, ServerConnection sc) throws SQLException;

    /**
     *
     * @param stmt
     * @param sc
     */
    SchemaConfig visitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, MycatSchemaStatVisitor visitor, ServerConnection sc) throws SQLException;

    /**
     * changeSql: add limit
     *
     * @param schema
     * @param rrs
     * @param stmt
     * @throws SQLNonTransientException
     */
    void changeSql(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, LayerCachePool cachePool) throws SQLException;

    /**
     * get parser info
     *
     * @return
     */
    DruidShardingParseInfo getCtx();

}
