/*
 * Copyright (C) 2016-2018 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.impl;

import com.actiontech.dble.cache.LayerCachePool;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.route.Procedure;
import com.actiontech.dble.route.ProcedureParameter;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteStrategy;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.sqlengine.mpp.LoadData;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

public abstract class AbstractRouteStrategy implements RouteStrategy {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRouteStrategy.class);

    @Override
    public RouteResultset route(SchemaConfig schema, int sqlType, String origSQL,
                                ServerConnection sc, LayerCachePool cachePool) throws SQLException {

        RouteResultset rrs = new RouteResultset(origSQL, sqlType);


        /*
         * debug mode and load data ,no cache
         */
        if (LOGGER.isDebugEnabled() && origSQL.startsWith(LoadData.LOAD_DATA_HINT)) {
            rrs.setCacheAble(false);
        }

        if (sqlType == ServerParse.CALL) {
            rrs.setCallStatement(true);
            Procedure procedure = parseProcedure(origSQL);
            rrs.setProcedure(procedure);
        }

        if (schema == null) {
            rrs = routeNormalSqlWithAST(null, origSQL, rrs, cachePool, sc);
        } else {
            if (sqlType == ServerParse.SHOW) {
                rrs.setStatement(origSQL);
                rrs = RouterUtil.routeToSingleNode(rrs, schema.getRandomDataNode());
            } else {
                rrs = routeNormalSqlWithAST(schema, origSQL, rrs, cachePool, sc);
            }
        }

        return rrs;
    }


    /**
     * routeNormalSqlWithAST
     */
    public abstract RouteResultset routeNormalSqlWithAST(SchemaConfig schema, String stmt, RouteResultset rrs,
                                                         LayerCachePool cachePool, ServerConnection sc) throws SQLException;


    private Procedure parseProcedure(String sql) {
        Procedure procedure = new Procedure();
        procedure.setOriginSql(sql);
        procedure.setResultList(false);
        SQLStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement statement = parser.parseStatement();
        if (statement instanceof SQLCallStatement) {
            parseProcedureForCall(procedure, sql, (SQLCallStatement) statement);
        }
        return procedure;
    }


    private void parseProcedureForCall(Procedure procedure, String query, SQLCallStatement statement) {
        SQLCallStatement sqlCallStatement = statement;
        procedure.setName(sqlCallStatement.getProcedureName().getSimpleName());
        List<SQLExpr> parameterList = sqlCallStatement.getParameters();
        for (int i1 = 0; i1 < parameterList.size(); i1++) {
            SQLExpr sqlExpr = parameterList.get(i1);
            String pName = sqlExpr.toString();
            ProcedureParameter parameter = new ProcedureParameter();
            parameter.setIndex(i1 + 1);
            parameter.setName(pName);
            parameter.setParameterType(ProcedureParameter.IN);
            if (pName.startsWith("@")) {
                procedure.getParameterMap().put(pName, parameter);
            } else {
                procedure.getParameterMap().put(String.valueOf(i1 + 1), parameter);
            }
        }
        procedure.setCallSql(query);
    }


}
