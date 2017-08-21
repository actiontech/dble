package io.mycat.backend.mysql.nio.handler.builder;

import io.mycat.backend.mysql.nio.handler.query.DMLResponseHandler;
import io.mycat.backend.mysql.nio.handler.query.impl.DistinctHandler;
import io.mycat.backend.mysql.nio.handler.query.impl.UnionHandler;
import io.mycat.plan.PlanNode;
import io.mycat.plan.node.MergeNode;
import io.mycat.server.NonBlockingSession;

import java.util.ArrayList;
import java.util.List;

class MergeNodeHandlerBuilder extends BaseHandlerBuilder {
    private MergeNode node;

    protected MergeNodeHandlerBuilder(NonBlockingSession session, MergeNode node, HandlerBuilder hBuilder) {
        super(session, node, hBuilder);
        this.node = node;
    }

    @Override
    protected List<DMLResponseHandler> buildPre() {
        List<DMLResponseHandler> pres = new ArrayList<DMLResponseHandler>();
        for (PlanNode child : node.getChildren()) {
            DMLResponseHandler ch = hBuilder.buildNode(session, child);
            pres.add(ch);
        }
        return pres;
    }

    @Override
    public void buildOwn() {
        UnionHandler uh = new UnionHandler(getSequenceId(), session, node.getComeInFields(), node.getChildren().size());
        addHandler(uh);
        if (node.isUnion()) {
            DistinctHandler dh = new DistinctHandler(getSequenceId(), session, node.getColumnsSelected());
            addHandler(dh);
        }
    }

}
