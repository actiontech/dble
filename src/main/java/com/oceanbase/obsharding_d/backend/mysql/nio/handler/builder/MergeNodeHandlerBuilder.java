/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.nio.handler.builder;

import com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.impl.DistinctHandler;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.impl.UnionHandler;
import com.oceanbase.obsharding_d.plan.node.MergeNode;
import com.oceanbase.obsharding_d.plan.node.PlanNode;
import com.oceanbase.obsharding_d.server.NonBlockingSession;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

class MergeNodeHandlerBuilder extends BaseHandlerBuilder {
    private final MergeNode node;
    private boolean optimizerMerge = false;

    protected MergeNodeHandlerBuilder(NonBlockingSession session, MergeNode node, HandlerBuilder hBuilder, boolean isExplain) {
        super(session, node, hBuilder, isExplain);
        this.node = node;
    }

    @Override
    protected boolean tryBuildWithCurrentNode(List<DMLResponseHandler> subQueryEndHandlers, Set<String> subQueryRouteNodes) throws SQLException {
        return false;
    }

    @Override
    protected List<DMLResponseHandler> buildPre() {
        List<DMLResponseHandler> pres = new ArrayList<>();
        for (PlanNode child : node.getChildren()) {
            BaseHandlerBuilder builder = hBuilder.getBuilder(session, child, isExplain);
            if (builder.getSubQueryBuilderList().size() > 0) {
                this.getSubQueryBuilderList().addAll(builder.getSubQueryBuilderList());
            }
            DMLResponseHandler ch = builder.getEndHandler();
            pres.add(ch);
        }
        if (this.getSubQueryBuilderList().size() == 0 && tryRouteToOneNode(pres)) {
            pres = null;
            optimizerMerge = true;
        }
        return pres;
    }

    @Override
    public void buildOwn() {
        if (optimizerMerge) {
            return;
        }
        UnionHandler uh = new UnionHandler(getSequenceId(), session, node.getComeInFields(), node.getChildren().size());
        addHandler(uh);
        if (node.isUnion()) {
            DistinctHandler dh = new DistinctHandler(getSequenceId(), session, node.getColumnsSelected());
            addHandler(dh);
        }
    }

}
