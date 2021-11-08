/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.builder;

import com.actiontech.dble.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.RenameFieldHandler;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.plan.node.PlanNode;
import com.actiontech.dble.plan.node.QueryNode;
import com.actiontech.dble.server.NonBlockingSession;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

class QueryNodeHandlerBuilder extends BaseHandlerBuilder {

    private final QueryNode node;
    private boolean optimizerMerge = false;

    protected QueryNodeHandlerBuilder(NonBlockingSession session,
                                      QueryNode node, HandlerBuilder hBuilder, boolean isExplain) {
        super(session, node, hBuilder, isExplain);
        this.node = node;
    }


    @Override
    public List<DMLResponseHandler> buildPre() {
        try {
            if (handleSubQueries()) {
                optimizerMerge = true;
                return null;
            }
        } catch (Exception e) {
            throw new MySQLOutPutException(ErrorCode.ER_QUERYHANDLER, "", "QueryNode buildOwn exception! Error:" + e.getMessage(), e);
        }
        List<DMLResponseHandler> pres = new ArrayList<>();
        PlanNode subNode = node.getChild();
        BaseHandlerBuilder builder = hBuilder.getBuilder(session, subNode, isExplain);
        if (builder.getSubQueryBuilderList().size() > 0) {
            this.getSubQueryBuilderList().addAll(builder.getSubQueryBuilderList());
        }
        DMLResponseHandler subHandler = builder.getEndHandler();
        pres.add(subHandler);
        return pres;
    }
    @Override
    protected boolean tryBuildWithCurrentNode(List<DMLResponseHandler> subQueryEndHandlers, Set<String> subQueryRouteNodes) {
        // tryRouteCurrentNode use node.copy(),it will replace sub-queries to 'NEED_REPLACE'
        BaseHandlerBuilder builder = hBuilder.getBuilder(session, node.getChild().copy(), isExplain);
        Set<String> routeNodes = HandlerBuilder.canRouteToNodes(builder.getEndHandler().getMerges());
        if (routeNodes != null && routeNodes.size() > 0) {
            Set<String> queryRouteNodes = tryRouteWithCurrentNode(subQueryRouteNodes, routeNodes.iterator().next(), routeNodes);
            if (queryRouteNodes.size() >= 1) {
                buildMergeHandlerWithSubQueries(subQueryEndHandlers, queryRouteNodes);
                return true;
            }
        }
        return false;
    }


    @Override
    public void buildOwn() {
        if (optimizerMerge) {
            return;
        }
        RenameFieldHandler rn = new RenameFieldHandler(getSequenceId(), session, node.getAlias(), node.getChild().type());
        addHandler(rn);
    }
}
