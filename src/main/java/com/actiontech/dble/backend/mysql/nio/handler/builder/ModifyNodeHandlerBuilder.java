/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.builder;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.nio.handler.builder.sqlvisitor.UpdateVisitor;
import com.actiontech.dble.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.MultiNodeUpdateHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.SendMakeHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.foreach.MergeUpdateHandler;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.subquery.ItemSubQuery;
import com.actiontech.dble.plan.common.item.subquery.UpdateItemSubQuery;
import com.actiontech.dble.plan.node.ModifyNode;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.route.parser.druid.ServerSchemaStatVisitor;
import com.actiontech.dble.route.parser.druid.impl.DruidUpdateParser;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.server.NonBlockingSession;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.google.common.collect.Lists;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

class ModifyNodeHandlerBuilder extends BaseHandlerBuilder {
    private final ModifyNode node;
    private long preHandlerSize;

    ModifyNodeHandlerBuilder(NonBlockingSession session, ModifyNode node, HandlerBuilder hBuilder, boolean isExplain) {
        super(session, node, hBuilder, isExplain);
        this.node = node;
        this.canPushDown = !node.existUnPushDownGroup();
        this.needWhereHandler = false;
    }

    @Override
    protected boolean tryBuildWithCurrentNode(List<DMLResponseHandler> subQueryEndHandlers, Set<String> subQueryRouteNodes) throws SQLException {
        return false;
    }

    @Override
    public List<DMLResponseHandler> buildPre() {
        if (node.getSubQueries().size() == 1) {
            // no optimizer
            List<DMLResponseHandler> subQueryEndHandlers;
            subQueryEndHandlers = getSubQueriesEndHandlers(node.getSubQueries());
            if (!isExplain) {
                // execute sub query sync
                executeSubQueries(subQueryEndHandlers);
            }
        }
        ItemSubQuery itemSubQuery = node.getSubQueries().get(0);
        if (!(itemSubQuery instanceof UpdateItemSubQuery)) {
            return new ArrayList<>();
        }
        UpdateItemSubQuery updateItemSubQuery = (UpdateItemSubQuery) itemSubQuery;
        if (updateItemSubQuery.getValue().isEmpty()) {
            this.isFastBack = true;
            return null;
        }
        return buildUpdateHandler(updateItemSubQuery);
    }

    private List<DMLResponseHandler> buildUpdateHandler(UpdateItemSubQuery updateItemSubQuery) {
        List<DMLResponseHandler> preHandlers = Lists.newArrayList();
        for (List<Item> valueItemList : updateItemSubQuery.getValue()) {
            UpdateVisitor updateVisitor = new UpdateVisitor(node, true, valueItemList, updateItemSubQuery.getSelect(), isExplain);
            RouteResultset rrs = updateVisitor.buildRouteResultset();

            SchemaConfig schemaConfig = DbleServer.getInstance().getConfig().getSchemas().get(this.session.getShardingService().getSchema());
            DruidUpdateParser updateParser = new DruidUpdateParser();
            SQLStatementParser parser = new MySqlStatementParser(rrs.getSrcStatement());
            SQLUpdateStatement updateStatement = (SQLUpdateStatement) parser.parseStatement();
            try {
                rrs = RouterUtil.routeFromParser(updateParser, schemaConfig, rrs, updateStatement, new ServerSchemaStatVisitor(schemaConfig.getName()), session.getShardingService(), isExplain);
            } catch (SQLException e) {
                throw new MySQLOutPutException(ErrorCode.ER_YES, "", e.getMessage());
            }

            RouteResultsetNode[] rrssArray = rrs.getNodes();
            hBuilder.checkRRSs(rrssArray);
            MultiNodeUpdateHandler mh = new MultiNodeUpdateHandler(getSequenceId(), session, rrssArray, session.getShardingService().isAutocommit() && !session.getShardingService().isTxStart());

            SendMakeHandler sh = new SendMakeHandler(getSequenceId(), session, node.getColumnsSelected(), schemaConfig.getName(), null, node.getAlias());
            mh.setNextHandler(sh);
            preHandlers.add(sh);
        }
        this.preHandlerSize = preHandlers.size();
        return preHandlers;
    }

    @Override
    public void buildOwn() {
        MergeUpdateHandler mergeUpdateHandler = new MergeUpdateHandler(getSequenceId(), session, preHandlerSize);
        addHandler(mergeUpdateHandler);
    }

}
