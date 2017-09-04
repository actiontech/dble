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
                                      QueryNode node, HandlerBuilder hBuilder) {
        super(session, node, hBuilder);
        this.node = node;
    }

    @Override
    public List<DMLResponseHandler> buildPre() {
        List<DMLResponseHandler> pres = new ArrayList<>();
        PlanNode subNode = node.getChild();
        DMLResponseHandler subHandler = hBuilder.buildNode(session, subNode);
        pres.add(subHandler);
        return pres;
    }

    @Override
    public void buildOwn() {
    }
}
