/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.nio.handler.builder;

import com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.impl.join.JoinInnerHandler;
import com.oceanbase.obsharding_d.plan.node.JoinInnerNode;
import com.oceanbase.obsharding_d.plan.node.PlanNode;
import com.oceanbase.obsharding_d.route.RouteResultset;
import com.oceanbase.obsharding_d.server.NonBlockingSession;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Created by szf on 2019/6/3.
 */
public class JoinInnerHandlerBuilder extends BaseHandlerBuilder {

    private final JoinInnerNode node;

    protected JoinInnerHandlerBuilder(NonBlockingSession session, JoinInnerNode node, HandlerBuilder hBuilder, boolean isExplain) {
        super(session, node, hBuilder, isExplain);
        this.node = node;
    }


    @Override
    protected boolean tryBuildWithCurrentNode(List<DMLResponseHandler> subQueryEndHandlers, Set<String> subQueryRouteNodes) throws SQLException {
        return false;
    }

    @Override
    public boolean canDoAsMerge() {
        return false;
    }


    @Override
    public void mergeBuild(RouteResultset rrs) {

    }

    @Override
    public List<DMLResponseHandler> buildPre() {
        if (node.getSubQueries().size() != 0) {
            // no optimizer
            List<DMLResponseHandler> subQueryEndHandlers;
            subQueryEndHandlers = getSubQueriesEndHandlers(node.getSubQueries());
            if (!isExplain) {
                // execute subquery sync
                executeSubQueries(subQueryEndHandlers);
            }
        }
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
