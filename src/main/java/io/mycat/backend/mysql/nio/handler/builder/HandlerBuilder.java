package io.mycat.backend.mysql.nio.handler.builder;

import io.mycat.backend.mysql.nio.handler.query.DMLResponseHandler;
import io.mycat.backend.mysql.nio.handler.query.impl.MultiNodeMergeHandler;
import io.mycat.backend.mysql.nio.handler.query.impl.OutputHandler;
import io.mycat.plan.PlanNode;
import io.mycat.plan.node.*;
import io.mycat.route.RouteResultsetNode;
import io.mycat.server.NonBlockingSession;
import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.Set;

public class HandlerBuilder {
    private static Logger logger = Logger.getLogger(HandlerBuilder.class);

    private PlanNode node;
    private NonBlockingSession session;
    private Set<RouteResultsetNode> rrsNodes = new HashSet<>();

    public HandlerBuilder(PlanNode node, NonBlockingSession session) {
        this.node = node;
        this.session = session;
    }

    public void checkRRSs(RouteResultsetNode[] rrssArray) {
        for (RouteResultsetNode rrss : rrssArray) {
            while (rrsNodes.contains(rrss)) {
                rrss.getMultiplexNum().incrementAndGet();
            }
            rrsNodes.add(rrss);
        }
    }

    /**
     * 启动一个节点下面的所有的启动节点
     */
    public static void startHandler(DMLResponseHandler handler) throws Exception {
        for (DMLResponseHandler startHandler : handler.getMerges()) {
            MultiNodeMergeHandler mergeHandler = (MultiNodeMergeHandler) startHandler;
            mergeHandler.execute();
        }
    }

    /**
     * 生成node链，返回endHandler
     *
     * @param planNode
     * @return
     */
    public DMLResponseHandler buildNode(NonBlockingSession nonBlockingSession, PlanNode planNode) {
        BaseHandlerBuilder builder = createBuilder(nonBlockingSession, planNode);
        builder.build();
        return builder.getEndHandler();
    }

    public void build(boolean hasNext) throws Exception {
        final long startTime = System.nanoTime();
        DMLResponseHandler endHandler = buildNode(session, node);
        OutputHandler fh = new OutputHandler(BaseHandlerBuilder.getSequenceId(), session, hasNext);
        endHandler.setNextHandler(fh);
        HandlerBuilder.startHandler(fh);
        long endTime = System.nanoTime();
        logger.info("HandlerBuilder.build cost:" + (endTime - startTime));
    }

    private BaseHandlerBuilder createBuilder(final NonBlockingSession nonBlockingSession, PlanNode planNode) {
        PlanNode.PlanNodeType i = planNode.type();
        if (i == PlanNode.PlanNodeType.TABLE) {
            return new TableNodeHandlerBuilder(nonBlockingSession, (TableNode) planNode, this);
        } else if (i == PlanNode.PlanNodeType.JOIN) {
            return new JoinNodeHandlerBuilder(nonBlockingSession, (JoinNode) planNode, this);
        } else if (i == PlanNode.PlanNodeType.MERGE) {
            return new MergeNodeHandlerBuilder(nonBlockingSession, (MergeNode) planNode, this);
        } else if (i == PlanNode.PlanNodeType.QUERY) {
            return new QueryNodeHandlerBuilder(nonBlockingSession, (QueryNode) planNode, this);
        } else if (i == PlanNode.PlanNodeType.NONAME) {
            return new NoNameNodeHandlerBuilder(nonBlockingSession, (NoNameNode) planNode, this);
        }
        throw new RuntimeException("not supported tree node type:" + planNode.type());
    }

}
