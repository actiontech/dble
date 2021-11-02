/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.builder;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.nio.handler.builder.sqlvisitor.PushDownVisitor;
import com.actiontech.dble.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.sharding.table.BaseTableConfig;
import com.actiontech.dble.config.model.sharding.table.GlobalTableConfig;
import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.node.TableNode;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

class TableNodeHandlerBuilder extends BaseHandlerBuilder {
    private final TableNode node;
    private final BaseTableConfig tableConfig;

    TableNodeHandlerBuilder(NonBlockingSession session, TableNode node, HandlerBuilder hBuilder, boolean isExplain) {
        super(session, node, hBuilder, isExplain);
        this.node = node;
        this.canPushDown = !node.existUnPushDownGroup();
        this.needWhereHandler = false;
        this.tableConfig = getTableConfig(node.getSchema(), node.getTableName());
    }

    @Override
    public List<DMLResponseHandler> buildPre() {
        return new ArrayList<>();
    }

    @Override
    public void buildOwn() {
        try {
            if (handleSubQueries()) return;
            // routeCurrentNode use node ,it will replace sub-queries to values
            RouteResultset rrs = tryRouteCurrentNode(node);
            buildMergeHandler(node, rrs.getNodes());
        } catch (Exception e) {
            throw new MySQLOutPutException(ErrorCode.ER_QUERYHANDLER, "", "TableNode buildOwn exception! Error:" + e.getMessage(), e);
        }
    }

    private RouteResultset tryRouteCurrentNode(TableNode testNode) throws SQLException {
        PushDownVisitor pdVisitor = new PushDownVisitor(testNode, true);
        RouteResultset rrs = pdVisitor.buildRouteResultset();

        SchemaConfig schemaConfig = DbleServer.getInstance().getConfig().getSchemas().get(testNode.getSchema());
        // maybe some node is view
        MergeBuilder mergeBuilder = new MergeBuilder(session, testNode);
        if (testNode.getAst() != null && testNode.getParent() == null) { // it's root
            return mergeBuilder.constructByStatement(rrs, testNode.getAst(), schemaConfig);
        } else {
            SQLStatementParser parser = new MySqlStatementParser(rrs.getSrcStatement());
            SQLSelectStatement select = (SQLSelectStatement) parser.parseStatement();
            return mergeBuilder.constructByStatement(rrs, select, schemaConfig);
        }
    }

    @Override
    protected void nestLoopBuild() {
        try {
            List<Item> filters = node.getNestLoopFilters();
            PushDownVisitor pdVisitor = new PushDownVisitor(node, true);
            if (filters == null || filters.isEmpty())
                throw new MySQLOutPutException(ErrorCode.ER_QUERYHANDLER, "", "unexpected exception!");
            List<RouteResultsetNode> rrssList = new ArrayList<>();
            MergeBuilder mergeBuilder = new MergeBuilder(session, node);
            SchemaConfig schemaConfig = DbleServer.getInstance().getConfig().getSchemas().get(node.getSchema());
            for (Item filter : filters) {
                node.setWhereFilter(filter);
                RouteResultset rrs = pdVisitor.buildRouteResultset();
                SQLStatementParser parser = new MySqlStatementParser(rrs.getSrcStatement());
                SQLSelectStatement select = (SQLSelectStatement) parser.parseStatement();
                RouteResultsetNode[] rrssArray = mergeBuilder.constructByStatement(rrs, select, schemaConfig).getNodes();
                rrssList.addAll(Arrays.asList(rrssArray));
            }
            if (tableConfig == null || tableConfig instanceof GlobalTableConfig) {
                if (filters.size() == 1) {
                    this.needCommon = false;
                }
            }
            RouteResultsetNode[] rrssArray = new RouteResultsetNode[rrssList.size()];
            rrssArray = rrssList.toArray(rrssArray);
            buildMergeHandler(node, rrssArray);
        } catch (Exception e) {
            throw new MySQLOutPutException(ErrorCode.ER_QUERYHANDLER, "", e.getMessage(), e);
        }
    }

    @Override
    protected boolean tryBuildWithCurrentNode(List<DMLResponseHandler> subQueryEndHandlers, Set<String> subQueryRouteNodes) throws SQLException {
        // tryRouteCurrentNode use node.copy(),it will replace sub-queries to 'NEED_REPLACE'
        RouteResultset rrs = tryRouteCurrentNode(node.copy());
        if (rrs.getNodes().length == 1) {
            Set<String> queryRouteNodes = tryRouteWithCurrentNode(subQueryRouteNodes, rrs.getNodes()[0].getName(), node.getNoshardNode());
            if (queryRouteNodes.size() >= 1) {
                buildMergeHandlerWithSubQueries(subQueryEndHandlers, queryRouteNodes);
                return true;
            }
        }
        return false;
    }
}
