/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.builder;

import com.actiontech.dble.backend.mysql.nio.handler.builder.sqlvisitor.PushDownVisitor;
import com.actiontech.dble.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.config.model.TableConfig.TableTypeEnum;
import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.node.TableNode;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class TableNodeHandlerBuilder extends BaseHandlerBuilder {
    private TableNode node;
    private TableConfig tableConfig = null;

    protected TableNodeHandlerBuilder(NonBlockingSession session, TableNode node, HandlerBuilder hBuilder, boolean isExplain) {
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
            MergeBuilder mergeBuilder = new MergeBuilder(session, node, needCommon, pdVisitor);
            String sql = null;
            if (node.getAst() != null && node.getParent() == null) { // it's root
                pdVisitor.visit();
                sql = pdVisitor.getSql().toString();
            }
            RouteResultsetNode[] rrssArray;
            // maybe some node is view
            if (sql == null) {
                rrssArray = mergeBuilder.construct().getNodes();
            } else {
                rrssArray = mergeBuilder.constructByStatement(sql, node.getAst()).getNodes();
            }
            this.needCommon = mergeBuilder.getNeedCommonFlag();
            buildMergeHandler(node, rrssArray);
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
            MergeBuilder mergeBuilder = new MergeBuilder(session, node, needCommon, pdVisitor);
            if (tableConfig == null || tableConfig.getTableType() == TableTypeEnum.TYPE_GLOBAL_TABLE) {
                for (Item filter : filters) {
                    node.setWhereFilter(filter);
                    RouteResultsetNode[] rrssArray = mergeBuilder.construct().getNodes();
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
                    RouteResultsetNode[] rrssArray = mergeBuilder.construct().getNodes();
                    rrssList.addAll(Arrays.asList(rrssArray));
                }
                if (tryGlobal) {
                    this.needCommon = mergeBuilder.getNeedCommonFlag();
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
