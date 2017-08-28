package io.mycat.route.impl;

import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlReplaceStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;

import io.mycat.cache.LayerCachePool;
import io.mycat.config.model.SchemaConfig;
import io.mycat.route.RouteResultset;
import io.mycat.route.parser.druid.DruidParser;
import io.mycat.route.parser.druid.DruidParserFactory;
import io.mycat.route.parser.druid.MycatSchemaStatVisitor;
import io.mycat.route.util.RouterUtil;
import io.mycat.server.ServerConnection;

public class DruidMycatRouteStrategy extends AbstractRouteStrategy {

    public static final Logger LOGGER = LoggerFactory.getLogger(DruidMycatRouteStrategy.class);

    @Override
    public SQLStatement parserSQL(String originSql) throws SQLSyntaxErrorException {
        SQLStatementParser parser = new MySqlStatementParser(originSql);

        /**
         * thrown SQL SyntaxError if parser error
         */
        try {
            List<SQLStatement> list = parser.parseStatementList();
            if (list.size() > 1) {
                throw new SQLSyntaxErrorException("MultiQueries is not supported,use single query instead ");
            }
            return list.get(0);
        } catch (Exception t) {
            LOGGER.error("routeNormalSqlWithAST", t);
            throw new SQLSyntaxErrorException(t);
        }
    }

    @Override
    public RouteResultset routeNormalSqlWithAST(SchemaConfig schema,
                                                String originSql, RouteResultset rrs, String charset,
                                                LayerCachePool cachePool, ServerConnection sc) throws SQLException {
        SQLStatement statement = parserSQL(originSql);
        /**
         * unsupported statement
         */
        checkUnSupportedStatement(statement);

        DruidParser druidParser = DruidParserFactory.create(statement, rrs.getSqlType());
        return RouterUtil.routeFromParser(druidParser, schema, rrs, statement, originSql, cachePool, new MycatSchemaStatVisitor(), sc);

    }


    /**
     *checkUnSupportedStatement
     *
     * @param statement
     * @throws SQLSyntaxErrorException
     */
    private void checkUnSupportedStatement(SQLStatement statement) throws SQLSyntaxErrorException {
        if (statement instanceof MySqlReplaceStatement) {
            throw new SQLSyntaxErrorException(" ReplaceStatement can't be supported,use insert into ...on duplicate key update... instead ");
        }
    }

}
