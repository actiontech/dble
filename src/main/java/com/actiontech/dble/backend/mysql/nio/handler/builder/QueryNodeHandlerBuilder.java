/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.builder;

import com.actiontech.dble.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.actiontech.dble.plan.PlanNode;
import com.actiontech.dble.plan.node.QueryNode;
import com.actiontech.dble.server.NonBlockingSession;

import java.util.ArrayList;
import java.util.List;

class QueryNodeHandlerBuilder extends BaseHandlerBuilder {

    private QueryNode node;

    protected QueryNodeHandlerBuilder(NonBlockingSession session,
                                      QueryNode node, HandlerBuilder hBuilder, boolean isExplain) {
        super(session, node, hBuilder, isExplain);
        this.node = node;
    }

    @Override
    protected void handleSubQueries() {
    }

    @Override
    public List<DMLResponseHandler> buildPre() {
        List<DMLResponseHandler> pres = new ArrayList<>();
        PlanNode subNode = node.getChild();
        DMLResponseHandler subHandler = hBuilder.buildNode(session, subNode, isExplain);
        pres.add(subHandler);
        return pres;
    }

    @Override
    public void buildOwn() {
    }
}
