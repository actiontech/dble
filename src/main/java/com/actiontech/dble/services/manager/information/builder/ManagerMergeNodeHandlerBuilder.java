/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.information.builder;

import com.actiontech.dble.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.DistinctHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.UnionHandler;
import com.actiontech.dble.plan.node.MergeNode;
import com.actiontech.dble.plan.node.PlanNode;
import com.actiontech.dble.services.manager.ManagerSession;

import java.util.ArrayList;
import java.util.List;

class ManagerMergeNodeHandlerBuilder extends ManagerBaseHandlerBuilder {
    private MergeNode node;

    ManagerMergeNodeHandlerBuilder(ManagerSession session, MergeNode node, ManagerHandlerBuilder hBuilder) {
        super(session, node, hBuilder);
        this.node = node;
    }

    @Override
    protected void handleSubQueries() {
    }

    @Override
    protected List<DMLResponseHandler> buildPre() {
        List<DMLResponseHandler> pres = new ArrayList<>();
        for (PlanNode child : node.getChildren()) {
            ManagerBaseHandlerBuilder builder = hBuilder.getBuilder(session, child);
            if (builder.getSubQueryBuilderList().size() > 0) {
                this.getSubQueryBuilderList().addAll(builder.getSubQueryBuilderList());
            }
            DMLResponseHandler ch = builder.getEndHandler();
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
