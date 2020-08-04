/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.builder;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.nio.handler.builder.sqlvisitor.PushDownVisitor;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.plan.node.PlanNode;
import com.actiontech.dble.plan.node.TableNode;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.parser.druid.ServerSchemaStatVisitor;
import com.actiontech.dble.route.parser.druid.impl.DruidSingleUnitSelectParser;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.parser.ServerParse;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class MergeBuilder {
    private PlanNode node;
    private NonBlockingSession session;
    private Map<String, SchemaConfig> schemaConfigMap = new HashMap<>();
    private PushDownVisitor pdVisitor;

    MergeBuilder(NonBlockingSession session, PlanNode node, PushDownVisitor pdVisitor) {
        this.node = node;
        this.session = session;
        this.schemaConfigMap.putAll(DbleServer.getInstance().getConfig().getSchemas());
        this.pdVisitor = pdVisitor;
    }

    /**
     * calculate the RouteResultset by Parser
     *
     * @return RouteResultset
     * @throws SQLException SQLException
     */
    public RouteResultset construct(SchemaConfig schemaConfig) throws SQLException {
        pdVisitor.visit();
        return constructByQuery(pdVisitor.getSql().toString(), pdVisitor.getMapTableToSimple(), schemaConfig);
    }

    private RouteResultset constructByQuery(String sql, Map<String, String> mapTableToSimple, SchemaConfig schemaConfig) throws SQLException {
        SQLStatementParser parser = new MySqlStatementParser(sql);
        SQLSelectStatement select = (SQLSelectStatement) parser.parseStatement();
        return constructByStatement(sql, mapTableToSimple, select, schemaConfig);
    }

    RouteResultset constructByStatement(String sql, Map<String, String> mapTableToSimple, SQLSelectStatement select, SchemaConfig schemaConfig) throws SQLException {
        RouteResultset rrs = new RouteResultset(sql, ServerParse.SELECT);
        String pushDownSQL = rrs.getStatement();
        for (Map.Entry<String, String> tableToSimple : mapTableToSimple.entrySet()) {
            pushDownSQL = pushDownSQL.replace(tableToSimple.getKey(), tableToSimple.getValue());
        }
        rrs.setStatement(pushDownSQL);
        rrs.setComplexSQL(true);
        Map<Pair<String, String>, SchemaConfig> tableConfigMap = new HashMap<>();
        for (TableNode tn : node.getReferedTableNodes()) {
            if (schemaConfigMap.get(tn.getSchema()) != null) {
                tableConfigMap.put(new Pair<>(tn.getSchema(), tn.getTableName()), schemaConfigMap.get(tn.getSchema()));
            }
        }
        DruidSingleUnitSelectParser druidParser = new DruidSingleUnitSelectParser();
        druidParser.setSchemaMap(tableConfigMap);
        return RouterUtil.routeFromParserComplex(schemaConfig, druidParser, tableConfigMap, rrs, select, new ServerSchemaStatVisitor(), session.getShardingService());
    }


}
