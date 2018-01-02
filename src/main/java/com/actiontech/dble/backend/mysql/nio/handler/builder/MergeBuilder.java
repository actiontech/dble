/*
 * Copyright (C) 2016-2018 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.builder;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.nio.handler.builder.sqlvisitor.PushDownVisitor;
import com.actiontech.dble.cache.LayerCachePool;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.plan.node.PlanNode;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.parser.druid.DruidParser;
import com.actiontech.dble.route.parser.druid.ServerSchemaStatVisitor;
import com.actiontech.dble.route.parser.druid.impl.DruidSingleUnitSelectParser;
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
    private boolean needCommonFlag;
    private PlanNode node;
    private NonBlockingSession session;
    protected Map<String, SchemaConfig> schemaConfigMap = new HashMap<>();
    private PushDownVisitor pdVisitor;

    MergeBuilder(NonBlockingSession session, PlanNode node, boolean needCommon, PushDownVisitor pdVisitor) {
        this.node = node;
        this.needCommonFlag = needCommon;
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
    public RouteResultset construct() throws SQLException {
        pdVisitor.visit();
        String sql = pdVisitor.getSql().toString();
        return constructByQuery(sql);
    }

    public RouteResultset constructByQuery(String sql) throws SQLException {
        SQLStatementParser parser = new MySqlStatementParser(sql);
        SQLSelectStatement select = (SQLSelectStatement) parser.parseStatement();
        return constructByStatement(sql, select);
    }

    public RouteResultset constructByStatement(String sql, SQLSelectStatement select) throws SQLException {
        ServerSchemaStatVisitor visitor = new ServerSchemaStatVisitor();
        DruidParser druidParser = new DruidSingleUnitSelectParser();

        RouteResultset rrs = new RouteResultset(sql, ServerParse.SELECT);
        LayerCachePool pool = DbleServer.getInstance().getRouterService().getTableId2DataNodeCache();
        SchemaConfig schemaConfig = schemaConfigMap.get(node.getReferedTableNodes().get(0).getSchema());
        return RouterUtil.routeFromParser(druidParser, schemaConfig, rrs, select, sql, pool, visitor, session.getSource());

    }

    /* -------------------- getter/setter -------------------- */
    boolean getNeedCommonFlag() {
        return needCommonFlag;
    }

}
