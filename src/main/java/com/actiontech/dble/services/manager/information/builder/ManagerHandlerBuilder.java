/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.information.builder;

import com.actiontech.dble.backend.mysql.nio.handler.builder.BaseHandlerBuilder;
import com.actiontech.dble.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.manager.ManagerBaseSelectHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.manager.ManagerOutputHandler;
import com.actiontech.dble.plan.node.*;
import com.actiontech.dble.services.manager.ManagerSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ManagerHandlerBuilder {
    private static Logger logger = LoggerFactory.getLogger(ManagerHandlerBuilder.class);

    private PlanNode node;
    private ManagerSession session;

    public ManagerHandlerBuilder(PlanNode node, ManagerSession session) {
        this.node = node;
        this.session = session;
    }

    public void build() throws Exception {
        final long startTime = System.nanoTime();
        ManagerBaseHandlerBuilder builder = getBuilder(session, node);
        DMLResponseHandler endHandler = builder.getEndHandler();
        ManagerOutputHandler fh = new ManagerOutputHandler(BaseHandlerBuilder.getSequenceId(), session);
        endHandler.setNextHandler(fh);
        ManagerHandlerBuilder.startHandler(fh);
        long endTime = System.nanoTime();
        if (logger.isDebugEnabled()) {
            logger.debug("HandlerBuilder.build cost:" + (endTime - startTime));
        }
    }

    private ManagerBaseHandlerBuilder createBuilder(final ManagerSession managerSession, PlanNode planNode) {
        PlanNode.PlanNodeType i = planNode.type();
        if (i == PlanNode.PlanNodeType.MANAGER_TABLE) {
            return new ManagerTableNodeHandlerBuilder(managerSession, (ManagerTableNode) planNode, this);
        } else if (i == PlanNode.PlanNodeType.JOIN) {
            return new ManagerJoinNodeHandlerBuilder(managerSession, (JoinNode) planNode, this);
        } else if (i == PlanNode.PlanNodeType.MERGE) {
            return new ManagerMergeNodeHandlerBuilder(managerSession, (MergeNode) planNode, this);
        } else if (i == PlanNode.PlanNodeType.QUERY) {
            return new ManagerQueryNodeHandlerBuilder(managerSession, (QueryNode) planNode, this);
        }
        throw new RuntimeException("not supported tree node type:" + planNode.type());
    }

    public ManagerBaseHandlerBuilder getBuilder(ManagerSession managerSession, PlanNode planNode) {
        ManagerBaseHandlerBuilder builder = createBuilder(managerSession, planNode);
        builder.build();
        return builder;
    }
    public static void startHandler(DMLResponseHandler handler) throws Exception {
        for (DMLResponseHandler startHandler : handler.getMerges()) {
            ManagerBaseSelectHandler baseHandler = (ManagerBaseSelectHandler) startHandler;
            baseHandler.execute();
        }
    }
}
