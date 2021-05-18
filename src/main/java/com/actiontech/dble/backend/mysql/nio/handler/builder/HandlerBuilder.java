/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.builder;

import com.actiontech.dble.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.BaseSelectHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.MultiNodeMergeHandler;
import com.actiontech.dble.plan.node.*;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.services.factorys.FinalHandlerFactory;
import com.actiontech.dble.singleton.TraceManager;
import com.actiontech.dble.util.StringUtil;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class HandlerBuilder {
    private static Logger logger = LoggerFactory.getLogger(HandlerBuilder.class);

    private PlanNode node;
    private NonBlockingSession session;
    private Set<RouteResultsetNode> rrsNodes = new HashSet<>();

    public HandlerBuilder(PlanNode node, NonBlockingSession session) {
        this.node = node;
        this.session = session;
    }

    /**
     * start all leaf handler of children of special handler
     */
    public static void startHandler(DMLResponseHandler handler) throws Exception {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(TraceManager.getThreadService(), "execute-complex-sql");
        try {
            for (DMLResponseHandler startHandler : handler.getMerges()) {
                MultiNodeMergeHandler mergeHandler = (MultiNodeMergeHandler) startHandler;
                mergeHandler.execute();
            }
        } finally {
            TraceManager.finishSpan(TraceManager.getThreadService(), traceObject);
        }
    }

    synchronized void checkRRSs(RouteResultsetNode[] rrssArray) {
        for (RouteResultsetNode rrss : rrssArray) {
            while (rrsNodes.contains(rrss)) {
                rrss.getMultiplexNum().incrementAndGet();
            }
            rrsNodes.add(rrss);
        }
    }

    synchronized void removeRrs(RouteResultsetNode rrsNode) {
        rrsNodes.remove(rrsNode);
    }

    public BaseHandlerBuilder getBuilder(NonBlockingSession nonBlockingSession, PlanNode planNode, boolean isExplain) {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(session.getShardingService(), "build-complex-sql");
        try {
            BaseHandlerBuilder builder = createBuilder(nonBlockingSession, planNode, isExplain);
            builder.build();
            return builder;
        } finally {
            TraceManager.finishSpan(session.getShardingService(), traceObject);
        }
    }

    public String build() throws Exception {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(session.getShardingService(), "build&execute-complex-sql");
        try {
            final long startTime = System.nanoTime();
            BaseHandlerBuilder builder = getBuilder(session, node, false);
            DMLResponseHandler endHandler = builder.getEndHandler();
            DMLResponseHandler fh = FinalHandlerFactory.createFinalHandler(session);
            endHandler.setNextHandler(fh);
            //set slave only into rrsNode
            for (DMLResponseHandler startHandler : fh.getMerges()) {
                MultiNodeMergeHandler mergeHandler = (MultiNodeMergeHandler) startHandler;
                for (BaseSelectHandler baseHandler : mergeHandler.getExeHandlers()) {
                    baseHandler.getRrss().setRunOnSlave(this.session.getComplexRrs().getRunOnSlave());
                }
            }
            session.endComplexRoute();
            if (!builder.isExistView() && !builder.isContainSubQuery()) {
                List<DMLResponseHandler> merges = Lists.newArrayList(builder.getEndHandler().getMerges());
                List<BaseHandlerBuilder> subQueryBuilderList = builder.getSubQueryBuilderList();
                subQueryBuilderList.stream().map(baseHandlerBuilder -> baseHandlerBuilder.getEndHandler().getMerges()).forEach(merges::addAll);
                String routeNode = canRouteToOneNode(merges);
                if (!StringUtil.isBlank(routeNode)) {
                    return routeNode;
                }
            }
            HandlerBuilder.startHandler(fh);
            session.endComplexExecute();
            long endTime = System.nanoTime();
            logger.debug("HandlerBuilder.build cost:" + (endTime - startTime));
            session.setTraceBuilder(builder);
        } finally {
            TraceManager.finishSpan(session.getShardingService(), traceObject);
        }
        return null;
    }

    /**
     * DBLE0REQ-504
     * According to the execution plan, judge whether it can be routed to the same node to simplify the query
     *
     * @param merges
     * @return
     */
    public static String canRouteToOneNode(List<DMLResponseHandler> merges) {
        String nodeName = null;
        for (DMLResponseHandler merge : merges) {
            if (merge instanceof MultiNodeMergeHandler) {
                RouteResultsetNode[] route = ((MultiNodeMergeHandler) merge).getRoute();
                if (null == route || route.length != 1) {
                    return null;
                }
                String name = route[0].getName();
                if (StringUtil.isBlank(nodeName)) {
                    nodeName = name;
                } else if (!nodeName.equals(name)) {
                    return null;
                }
            }
        }
        return nodeName;
    }

    private BaseHandlerBuilder createBuilder(final NonBlockingSession nonBlockingSession, PlanNode planNode, boolean isExplain) {
        PlanNode.PlanNodeType i = planNode.type();
        if (i == PlanNode.PlanNodeType.TABLE) {
            return new TableNodeHandlerBuilder(nonBlockingSession, (TableNode) planNode, this, isExplain);
        } else if (i == PlanNode.PlanNodeType.JOIN) {
            return new JoinNodeHandlerBuilder(nonBlockingSession, (JoinNode) planNode, this, isExplain);
        } else if (i == PlanNode.PlanNodeType.MERGE) {
            return new MergeNodeHandlerBuilder(nonBlockingSession, (MergeNode) planNode, this, isExplain);
        } else if (i == PlanNode.PlanNodeType.QUERY) {
            return new QueryNodeHandlerBuilder(nonBlockingSession, (QueryNode) planNode, this, isExplain);
        } else if (i == PlanNode.PlanNodeType.NONAME) {
            return new NoNameNodeHandlerBuilder(nonBlockingSession, (NoNameNode) planNode, this, isExplain);
        } else if (i == PlanNode.PlanNodeType.JOIN_INNER) {
            return new JoinInnerHandlerBuilder(nonBlockingSession, (JoinInnerNode) planNode, this, isExplain);
        }
        throw new RuntimeException("not supported tree node type:" + planNode.type());
    }

}
