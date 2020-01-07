/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.builder;

import com.actiontech.dble.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.RenameFieldHandler;
import com.actiontech.dble.plan.node.PlanNode;
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
        handleBlockingSubQuery();
    }

    @Override
    public List<DMLResponseHandler> buildPre() {
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
    public void buildOwn() {
        RenameFieldHandler rn = new RenameFieldHandler(getSequenceId(), session, node.getAlias(), node.getChild().type());
        addHandler(rn);
    }
}
