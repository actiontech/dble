/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.builder;

import com.actiontech.dble.backend.mysql.nio.handler.builder.sqlvisitor.GlobalVisitor;
import com.actiontech.dble.backend.mysql.nio.handler.query.BaseDMLHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.MultiNodeEasyMergeHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.MultiNodeMergeHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.SendMakeHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.TempTableHandler;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.plan.node.*;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.services.factorys.FinalHandlerFactory;
import com.actiontech.dble.singleton.TraceManager;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class HandlerBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(HandlerBuilder.class);

    private final PlanNode node;
    private final NonBlockingSession session;
    private final Set<RouteResultsetNode> rrsNodes = new HashSet<>();

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
            //shardingNode-index
            while (rrsNodes.stream().anyMatch(rrsNode -> Objects.equals(rrsNode.getMultiplexNum().get(), rrss.getMultiplexNum().get()) && Objects.equals(rrsNode.getName(), rrss.getName()))) {
                rrss.getMultiplexNum().incrementAndGet();
            }
            //repeatTable-index
            while (rrsNodes.stream().anyMatch(rrsNode -> rrsNode.contains(rrsNode.getTableSet(), rrss.getTableSet()) && Objects.equals(rrsNode.getName(), rrss.getName()) && Objects.equals(rrsNode.getRepeatTableIndex().get(), rrss.getRepeatTableIndex().get()))) {
                rrss.getRepeatTableIndex().incrementAndGet();
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

    public RouteResultsetNode build(boolean isHaveHintPlan2Inner) throws Exception {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(session.getShardingService(), "build&execute-complex-sql");
        try {
            final long startTime = System.nanoTime();
            BaseHandlerBuilder builder = getBuilder(session, node, false);
            if (builder.isFastBack()) {
                //fast back
                session.getShardingService().write(OkPacket.getDefault());
                return null;
            }
            DMLResponseHandler endHandler = builder.getEndHandler();
            DMLResponseHandler fh = FinalHandlerFactory.createFinalHandler(session);
            endHandler.setNextHandler(fh);
            //set slave only into rrsNode
            HashSet<String> nodeSet = Sets.newHashSet();
            boolean nodeRepeat = false;
            for (DMLResponseHandler startHandler : fh.getMerges()) {
                MultiNodeMergeHandler mergeHandler = (MultiNodeMergeHandler) startHandler;
                if (Objects.nonNull(mergeHandler.getRoute())) {
                    for (RouteResultsetNode routeResultsetNode : mergeHandler.getRoute()) {
                        if (!nodeSet.add(routeResultsetNode.getName())) {
                            nodeRepeat = true;
                        }
                    }
                }
                for (BaseDMLHandler baseHandler : mergeHandler.getExeHandlers()) {
                    baseHandler.getRrss().setRunOnSlave(this.session.getComplexRrs().getRunOnSlave());
                }
            }

            if (nodeRepeat) {
                for (DMLResponseHandler startHandler : fh.getMerges()) {
                    MultiNodeMergeHandler mergeHandler = (MultiNodeMergeHandler) startHandler;
                    for (RouteResultsetNode routeResultsetNode : mergeHandler.getRoute()) {
                        routeResultsetNode.setNodeRepeat(true);
                    }
                }
            }
            session.trace(t -> t.endComplexRoute());

            RouteResultsetNode routeSingleNode = getTryRouteSingleNode(builder, isHaveHintPlan2Inner);
            if (routeSingleNode != null)
                return routeSingleNode;

            HandlerBuilder.startHandler(fh);
            session.trace(t -> t.endComplexExecute());
            long endTime = System.nanoTime();
            LOGGER.debug("HandlerBuilder.build cost:" + (endTime - startTime));
            session.trace(t -> t.setTraceBuilder(builder));
        } finally {
            TraceManager.finishSpan(session.getShardingService(), traceObject);
        }
        return null;
    }

    public boolean canAsWholeToSingle(List<DMLResponseHandler> merges) {
        if (merges.size() != 1 || nestLoopCheck(merges))
            return false;
        return true;
    }

    public static boolean nestLoopCheck(List<DMLResponseHandler> merges) {
        DMLResponseHandler next = merges.get(0).getNextHandler();
        while (next != null) {
            if (next instanceof TempTableHandler || (next instanceof SendMakeHandler && !((SendMakeHandler) next).getTableHandlers().isEmpty()))
                return true;
            next = next.getNextHandler();
        }
        return false;
    }

    // check whether the SQL can be directly sent to a single node
    private RouteResultsetNode getTryRouteSingleNode(BaseHandlerBuilder builder, boolean isHaveHintPlan2Inner) {
        RouteResultsetNode routeNode = null;
        if (canAsWholeToSingle(builder.getEndHandler().getMerges()) && builder.getSubQueryBuilderList().size() == 0) {
            RouteResultsetNode[] routes = ((MultiNodeMergeHandler) (builder.getEndHandler().getMerges().get(0))).getRoute();
            if (routes.length == 1) {
                routeNode = routes[0];
            }
        }
        if (routeNode == null) return null;

        Set<String> tableSet = Sets.newHashSet();
        for (RouteResultsetNode routeResultsetNode : rrsNodes) {
            Set<String> set = routeResultsetNode.getTableSet();
            if (null != set) {
                tableSet.addAll(set);
            }
        }
        String sql = isHaveHintPlan2Inner ? routeNode.getStatement() : node.getSql();
        if (builder.isExistView() || builder.isContainSubQuery(node)) {
            GlobalVisitor visitor = new GlobalVisitor(node, true, false);
            visitor.visit();
            sql = visitor.getSql().toString();
            Map<String, String> mapTableToSimple = visitor.getMapTableToSimple();
            for (Map.Entry<String, String> tableToSimple : mapTableToSimple.entrySet()) {
                sql = sql.replace(tableToSimple.getKey(), tableToSimple.getValue());
            }
        }
        return new RouteResultsetNode(routeNode.getName(), ServerParse.SELECT, sql, tableSet);
    }

    /**
     * DBLE0REQ-504
     * According to the execution plan, judge whether it can be routed to the same node to simplify the query
     */
    public static String canRouteToOneNode(List<DMLResponseHandler> merges) {
        Set<String> globalBackNodes = canRouteToNodes(merges);
        if (globalBackNodes == null) return null;
        else if (globalBackNodes.size() == 1) {
            return globalBackNodes.iterator().next();
        } else {
            return RouterUtil.getRandomShardingNode(globalBackNodes);
        }
    }

    public static Set<String> canRouteToNodes(List<DMLResponseHandler> merges) {
        String nodeName = null;
        Set<String> globalBackNodes = null;
        for (DMLResponseHandler merge : merges) {
            if (merge instanceof MultiNodeMergeHandler) {
                RouteResultsetNode[] route = ((MultiNodeMergeHandler) merge).getRoute();
                if (null == route || route.length != 1) {
                    return null;
                }
                Set<String> tryGlobalBackNodes = null;
                if (merge instanceof MultiNodeEasyMergeHandler) {
                    tryGlobalBackNodes = ((MultiNodeEasyMergeHandler) merge).getGlobalBackNodes();
                }

                String tryNodeName = route[0].getName();
                if (nodeName == null) {
                    // current table is non-global table
                    if (tryGlobalBackNodes == null) {
                        // current table is first table
                        if (globalBackNodes == null) {
                            nodeName = tryNodeName;
                            globalBackNodes = new HashSet<>();
                            globalBackNodes.add(nodeName);
                        } else if (globalBackNodes.contains(tryNodeName)) {
                            // current table is not first table, and all route has tryNodeName
                            nodeName = tryNodeName;
                            globalBackNodes.clear();
                            globalBackNodes.add(tryNodeName);
                        } else { // tables before it do not contain tryNodeName
                            return null;
                        }
                    } else { // current table is global table
                        // current table is first table
                        if (globalBackNodes == null) {
                            globalBackNodes = tryGlobalBackNodes;
                        } else {
                            globalBackNodes.retainAll(tryGlobalBackNodes); // retain current table nodes
                            if (globalBackNodes.size() == 0) {
                                return null;
                            }
                        }
                    }
                } else if (!nodeName.equals(tryNodeName)) {
                    // tryNodeName can not be changed ,because tryGlobalBackNodes not contains nodeName
                    if (tryGlobalBackNodes == null || !tryGlobalBackNodes.contains(nodeName)) {
                        return null;
                    }
                }
            }
        }
        return globalBackNodes;
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
        } else if (i == PlanNode.PlanNodeType.MODIFY) {
            return new ModifyNodeHandlerBuilder(nonBlockingSession, (ModifyNode) planNode, this, isExplain);
        }
        throw new RuntimeException("not supported tree node type:" + planNode.type());
    }

    public Set<RouteResultsetNode> getRrsNodes() {
        return rrsNodes;
    }
}
