/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.information.builder;

import com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.impl.RenameFieldHandler;
import com.oceanbase.obsharding_d.plan.node.PlanNode;
import com.oceanbase.obsharding_d.plan.node.QueryNode;
import com.oceanbase.obsharding_d.services.manager.ManagerSession;

import java.util.ArrayList;
import java.util.List;

class ManagerQueryNodeHandlerBuilder extends ManagerBaseHandlerBuilder {

    private QueryNode node;

    ManagerQueryNodeHandlerBuilder(ManagerSession session,
                                   QueryNode node, ManagerHandlerBuilder hBuilder) {
        super(session, node, hBuilder);
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
        ManagerBaseHandlerBuilder builder = hBuilder.getBuilder(session, subNode);
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
