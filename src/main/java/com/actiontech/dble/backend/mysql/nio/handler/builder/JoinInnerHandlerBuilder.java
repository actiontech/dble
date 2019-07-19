package com.actiontech.dble.backend.mysql.nio.handler.builder;

import com.actiontech.dble.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.join.JoinInnerHandler;
import com.actiontech.dble.plan.node.JoinInnerNode;
import com.actiontech.dble.plan.node.PlanNode;
import com.actiontech.dble.server.NonBlockingSession;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by szf on 2019/6/3.
 */
public class JoinInnerHandlerBuilder extends BaseHandlerBuilder {

    private JoinInnerNode node;

    protected JoinInnerHandlerBuilder(NonBlockingSession session, JoinInnerNode node, HandlerBuilder hBuilder, boolean isExplain) {
        super(session, node, hBuilder, isExplain);
        this.node = node;
    }


    @Override
    public boolean canDoAsMerge() {
        return false;
    }

    @Override
    protected void handleSubQueries() {
        handleBlockingSubQuery();
    }

    @Override
    public void mergeBuild() {

    }

    @Override
    public List<DMLResponseHandler> buildPre() {
        List<DMLResponseHandler> pres = new ArrayList<>();
        PlanNode left = node.getLeftNode();
        PlanNode right = node.getRightNode();
        DMLResponseHandler lh = buildJoinChild(left, true);
        pres.add(lh);
        DMLResponseHandler rh = buildJoinChild(right, false);
        pres.add(rh);
        return pres;
    }

    private DMLResponseHandler buildJoinChild(PlanNode child, boolean isLeft) {
        BaseHandlerBuilder builder = hBuilder.getBuilder(session, child, isExplain);
        if (builder.getSubQueryBuilderList().size() > 0) {
            this.getSubQueryBuilderList().addAll(builder.getSubQueryBuilderList());
        }
        DMLResponseHandler endHandler = builder.getEndHandler();
        if (isLeft) {
            endHandler.setLeft(true);
        }
        return endHandler;
    }

    @Override
    public void buildOwn() {
        JoinInnerHandler jh = new JoinInnerHandler(getSequenceId(), session, false,
                new ArrayList<>(), new ArrayList<>(), null);
        addHandler(jh);
    }


}
