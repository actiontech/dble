/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.handler;

import com.actiontech.dble.cache.LayerCachePool;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.route.*;
import com.actiontech.dble.route.factory.RouteStrategyFactory;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.parser.ServerParse;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLNumberExpr;
import com.alibaba.druid.sql.ast.expr.SQLTextLiteralExpr;
import com.alibaba.druid.sql.ast.expr.SQLValuableExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;

import java.sql.SQLException;
import java.sql.Types;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * HintSQLHandler
 */
public class HintSQLHandler implements HintHandler {

    private RouteStrategy routeStrategy;

    public HintSQLHandler() {
        this.routeStrategy = RouteStrategyFactory.getRouteStrategy();
    }

    @Override
    public RouteResultset route(SchemaConfig schema, int sqlType, String realSQL, ServerConnection sc,
                                LayerCachePool cachePool, String hintSQLValue, int hintSqlType, Map hintMap)
            throws SQLException {

        RouteResultset rrs = routeStrategy.route(schema, hintSqlType,
                hintSQLValue, sc, cachePool);

        // replace the sql of RRS
        RouteResultsetNode[] oldRsNodes = rrs.getNodes();
        RouteResultsetNode[] newRrsNodes = new RouteResultsetNode[oldRsNodes.length];
        for (int i = 0; i < newRrsNodes.length; i++) {
            newRrsNodes[i] = new RouteResultsetNode(oldRsNodes[i].getName(),
                    oldRsNodes[i].getSqlType(), realSQL);
        }
        rrs.setNodes(newRrsNodes);

        //  can't judge the cal statement by parser
        if (ServerParse.CALL == sqlType) {
            rrs.setCallStatement(true);

            Procedure procedure = parseProcedure(realSQL, hintMap);
            rrs.setProcedure(procedure);
            //    String sql=procedure.toChangeCallSql(null);
            String sql = realSQL;
            for (RouteResultsetNode node : rrs.getNodes()) {
                node.setStatement(sql);
            }

        }

        return rrs;
    }


    private Procedure parseProcedure(String sql, Map hintMap) {
        boolean fields = hintMap.containsKey("list_fields");
        boolean isResultList = "list".equals(hintMap.get("result_type")) || fields;
        Procedure procedure = new Procedure();
        procedure.setOriginSql(sql);
        procedure.setResultList(isResultList);
        List<String> sqlList = Splitter.on(";").trimResults().splitToList(sql);
        Set<String> outSet = new HashSet<>();
        for (int i = sqlList.size() - 1; i >= 0; i--) {
            String query = sqlList.get(i);
            if (Strings.isNullOrEmpty(query)) {
                continue;
            }
            SQLStatementParser parser = new MySqlStatementParser(query);
            SQLStatement statement = parser.parseStatement();
            if (statement instanceof SQLSelectStatement) {
                parseProcedureForSelect(procedure, outSet, query, (SQLSelectStatement) statement);
            } else if (statement instanceof SQLCallStatement) {
                parseProcedureForCall(procedure, outSet, query, (SQLCallStatement) statement);
            } else if (statement instanceof SQLSetStatement) {
                parseProcedureForSet(procedure, query, (SQLSetStatement) statement);
            }

        }
        if (fields) {
            String listFieldsStr = (String) hintMap.get("list_fields");
            List<String> listFields = Splitter.on(",").trimResults().splitToList(listFieldsStr);
            for (String field : listFields) {
                if (!procedure.getParameterMap().containsKey(field)) {
                    ProcedureParameter parameter = new ProcedureParameter();
                    parameter.setParameterType(ProcedureParameter.OUT);
                    parameter.setName(field);
                    parameter.setJdbcType(-10);
                    parameter.setIndex(procedure.getParameterMap().size() + 1);
                    procedure.getParameterMap().put(field, parameter);
                }
            }
            procedure.getListFields().addAll(listFields);
        }
        return procedure;
    }

    private void parseProcedureForSet(Procedure procedure, String query, SQLSetStatement statement) {
        procedure.setSetSql(query);
        SQLSetStatement setStatement = statement;
        List<SQLAssignItem> sets = setStatement.getItems();
        for (SQLAssignItem set : sets) {
            String name = set.getTarget().toString();
            SQLExpr value = set.getValue();
            ProcedureParameter parameter = procedure.getParameterMap().get(name);
            if (parameter != null) {
                if (value instanceof SQLIntegerExpr) {
                    parameter.setValue(((SQLIntegerExpr) value).getNumber());
                    parameter.setJdbcType(Types.INTEGER);
                } else if (value instanceof SQLNumberExpr) {
                    parameter.setValue(((SQLNumberExpr) value).getNumber());
                    parameter.setJdbcType(Types.NUMERIC);
                } else if (value instanceof SQLTextLiteralExpr) {
                    parameter.setValue(((SQLTextLiteralExpr) value).getText());
                    parameter.setJdbcType(Types.VARCHAR);
                } else if (value instanceof SQLValuableExpr) {
                    parameter.setValue(((SQLValuableExpr) value).getValue());
                    parameter.setJdbcType(Types.VARCHAR);
                }
            }
        }
    }

    private void parseProcedureForCall(Procedure procedure, Set<String> outSet, String query, SQLCallStatement statement) {
        SQLCallStatement sqlCallStatement = statement;
        procedure.setName(sqlCallStatement.getProcedureName().getSimpleName());
        List<SQLExpr> paramterList = sqlCallStatement.getParameters();
        for (int i1 = 0; i1 < paramterList.size(); i1++) {
            SQLExpr sqlExpr = paramterList.get(i1);
            String pName = sqlExpr.toString();
            String pType = outSet.contains(pName) ? ProcedureParameter.OUT : ProcedureParameter.IN;
            ProcedureParameter parameter = new ProcedureParameter();
            parameter.setIndex(i1 + 1);
            parameter.setName(pName);
            parameter.setParameterType(pType);
            if (pName.startsWith("@")) {
                procedure.getParameterMap().put(pName, parameter);
            } else {
                procedure.getParameterMap().put(String.valueOf(i1 + 1), parameter);
            }


        }
        procedure.setCallSql(query);
    }

    private void parseProcedureForSelect(Procedure procedure, Set<String> outSet, String query, SQLSelectStatement statement) {
        MySqlSelectQueryBlock selectQuery = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
        if (selectQuery != null) {
            List<SQLSelectItem> selectItems = selectQuery.getSelectList();
            for (SQLSelectItem selectItem : selectItems) {
                String select = selectItem.toString();
                outSet.add(select);
                procedure.getSelectColumns().add(select);
            }
        }
        procedure.setSelectSql(query);
    }
}
