/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.builder;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.nio.handler.builder.sqlvisitor.PushDownVisitor;
import com.actiontech.dble.cache.LayerCachePool;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.plan.PlanNode;
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

public class MergeBuilder {
    private boolean needCommonFlag;
    private boolean needSendMakerFlag;
    private PlanNode node;
    private NonBlockingSession session;
    private ServerConfig config;
    private PushDownVisitor pdVisitor;

    public MergeBuilder(NonBlockingSession session, PlanNode node, boolean needCommon, boolean needSendMaker,
                        PushDownVisitor pdVisitor) {
        this.node = node;
        this.needCommonFlag = needCommon;
        this.needSendMakerFlag = needSendMaker;
        this.session = session;
        this.config = DbleServer.getInstance().getConfig();
        this.pdVisitor = pdVisitor;
    }

    /**
     * calculate the RouteResultset by Parser
     *
     * @return
     * @throws SQLException
     */
    public RouteResultset construct() throws SQLException {
        pdVisitor.visit();
        String sql = pdVisitor.getSql().toString();
        SQLStatementParser parser = new MySqlStatementParser(sql);
        SQLSelectStatement select = (SQLSelectStatement) parser.parseStatement();
        ServerSchemaStatVisitor visitor = new ServerSchemaStatVisitor();
        DruidParser druidParser = new DruidSingleUnitSelectParser();

        RouteResultset rrs = new RouteResultset(sql, ServerParse.SELECT);
        LayerCachePool pool = DbleServer.getInstance().getRouterService().getTableId2DataNodeCache();
        SchemaConfig schemaConfig = config.getSchemas().get(node.getReferedTableNodes().get(0).getSchema());
        return RouterUtil.routeFromParser(druidParser, schemaConfig, rrs, select, sql, pool, visitor, session.getSource());

    }

    /* -------------------- getter/setter -------------------- */
    public boolean getNeedCommonFlag() {
        return needCommonFlag;
    }

    public boolean getNeedSendMakerFlag() {
        return needSendMakerFlag;
    }

}
