/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.builder;

import com.actiontech.dble.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.DistinctHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.UnionHandler;
import com.actiontech.dble.plan.PlanNode;
import com.actiontech.dble.plan.node.MergeNode;
import com.actiontech.dble.server.NonBlockingSession;

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
        List<DMLResponseHandler> pres = new ArrayList<>();
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
