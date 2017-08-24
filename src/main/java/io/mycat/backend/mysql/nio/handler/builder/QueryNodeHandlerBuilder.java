package io.mycat.backend.mysql.nio.handler.builder;

import io.mycat.backend.mysql.nio.handler.query.DMLResponseHandler;
import io.mycat.plan.PlanNode;
import io.mycat.plan.node.QueryNode;
import io.mycat.server.NonBlockingSession;

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
