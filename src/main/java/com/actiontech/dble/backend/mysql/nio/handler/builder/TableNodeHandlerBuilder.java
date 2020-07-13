/*
 * Copyright (C) 2016-2020 ActionTech.
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

import java.util.*;

class TableNodeHandlerBuilder extends BaseHandlerBuilder {
    private TableNode node;
    private BaseTableConfig tableConfig = null;

    TableNodeHandlerBuilder(NonBlockingSession session, TableNode node, HandlerBuilder hBuilder, boolean isExplain) {
        super(session, node, hBuilder, isExplain);
        this.node = node;
        this.canPushDown = !node.existUnPushDownGroup();
        this.needWhereHandler = false;
        this.tableConfig = getTableConfig(node.getSchema(), node.getTableName());
    }

    @Override
    protected void handleSubQueries() {
        handleBlockingSubQuery();
    }

    @Override
    public List<DMLResponseHandler> buildPre() {
        return new ArrayList<>();
    }

    @Override
    public void buildOwn() {
        try {
            PushDownVisitor pdVisitor = new PushDownVisitor(node, true);
            MergeBuilder mergeBuilder = new MergeBuilder(session, node, pdVisitor);
            String sql = null;
            Map<String, String> mapTableToSimple = new HashMap<>();
            if (node.getAst() != null && node.getParent() == null) { // it's root
                pdVisitor.visit();
                sql = pdVisitor.getSql().toString();
                mapTableToSimple = pdVisitor.getMapTableToSimple();
            }
            SchemaConfig schemaConfig = DbleServer.getInstance().getConfig().getSchemas().get(node.getSchema());
            // maybe some node is view
            RouteResultset rrs;
            if (sql == null) {
                rrs = mergeBuilder.construct(schemaConfig);
            } else {
                rrs = mergeBuilder.constructByStatement(sql, mapTableToSimple, node.getAst(), schemaConfig);
            }
            buildMergeHandler(node, rrs.getNodes());
        } catch (Exception e) {
            throw new MySQLOutPutException(ErrorCode.ER_QUERYHANDLER, "", "table node buildOwn exception! Error:" + e.getMessage(), e);
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
            MergeBuilder mergeBuilder = new MergeBuilder(session, node, pdVisitor);
            SchemaConfig schemaConfig = DbleServer.getInstance().getConfig().getSchemas().get(node.getSchema());
            if (tableConfig == null || tableConfig instanceof GlobalTableConfig) {
                for (Item filter : filters) {
                    node.setWhereFilter(filter);
                    RouteResultsetNode[] rrssArray = mergeBuilder.construct(schemaConfig).getNodes();
                    rrssList.addAll(Arrays.asList(rrssArray));
                }
                if (filters.size() == 1) {
                    this.needCommon = false;
                }
            } else {
                boolean tryGlobal = filters.size() == 1;
                for (Item filter : filters) {
                    node.setWhereFilter(filter);
                    pdVisitor.visit();
                    RouteResultsetNode[] rrssArray = mergeBuilder.construct(schemaConfig).getNodes();
                    rrssList.addAll(Arrays.asList(rrssArray));
                }
            }
            RouteResultsetNode[] rrssArray = new RouteResultsetNode[rrssList.size()];
            rrssArray = rrssList.toArray(rrssArray);
            buildMergeHandler(node, rrssArray);
        } catch (Exception e) {
            throw new MySQLOutPutException(ErrorCode.ER_QUERYHANDLER, "", e.getMessage(), e);
        }
    }
}
