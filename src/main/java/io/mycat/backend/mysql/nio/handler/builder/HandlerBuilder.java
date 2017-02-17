package io.mycat.backend.mysql.nio.handler.builder;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import io.mycat.backend.mysql.nio.handler.query.DMLResponseHandler;
import io.mycat.backend.mysql.nio.handler.query.impl.MultiNodeMergeHandler;
import io.mycat.backend.mysql.nio.handler.query.impl.OutputHandler;
import io.mycat.plan.PlanNode;
import io.mycat.plan.node.JoinNode;
import io.mycat.plan.node.MergeNode;
import io.mycat.plan.node.NoNameNode;
import io.mycat.plan.node.QueryNode;
import io.mycat.plan.node.TableNode;
import io.mycat.route.RouteResultsetNode;
import io.mycat.server.NonBlockingSession;

public class HandlerBuilder {
	private static Logger logger = Logger.getLogger(HandlerBuilder.class);

	private PlanNode node;
	private NonBlockingSession session;
	private OutputHandler fh;
	private Set<RouteResultsetNode> rrsNodes = new HashSet<RouteResultsetNode>();
	public HandlerBuilder(PlanNode node, NonBlockingSession session) {
		this.node = node;
		this.session = session;
	}

	public List<RouteResultsetNode> buildRouteSources() {
		List<RouteResultsetNode> list = new ArrayList<RouteResultsetNode>();
		BaseHandlerBuilder builder = createBuilder(session, node, this);
		builder.build();
		fh = new OutputHandler(BaseHandlerBuilder.getSequenceId(), session, false);
		DMLResponseHandler endHandler = builder.getEndHandler();
		endHandler.setNextHandler(fh);
		for (DMLResponseHandler handler : fh.getMerges()) {
			MultiNodeMergeHandler mergeHandler = (MultiNodeMergeHandler) handler;
			for (int i = 0; i < mergeHandler.getRouteSources().length; i++) {
				list.add(mergeHandler.getRouteSources()[i]);
			}
		}
		return list;
	}
	
	public void checkRRSS(RouteResultsetNode[] rrssArray) {
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
	 * @param node
	 * @return
	 */
	public DMLResponseHandler buildNode(NonBlockingSession session, PlanNode node) {
		BaseHandlerBuilder builder = createBuilder(session, node, this);
		builder.build();
		return builder.getEndHandler();
	}

	public void build(boolean hasNext) throws Exception {
		long startTime = System.nanoTime();
		DMLResponseHandler endHandler = buildNode(session, node);
		fh = new OutputHandler(BaseHandlerBuilder.getSequenceId(), session, hasNext);
		endHandler.setNextHandler(fh);
		HandlerBuilder.startHandler(fh);
		long endTime = System.nanoTime();
		logger.info("HandlerBuilder.build cost:" + (endTime - startTime));
	}

	private BaseHandlerBuilder createBuilder(final NonBlockingSession session, PlanNode node, HandlerBuilder context) {
		switch (node.type()) {
		case TABLE: {
			return new TableNodeHandlerBuilder(session, (TableNode) node, this);
		}
		case JOIN: {
			return new JoinNodeHandlerBuilder(session, (JoinNode) node, this);
		}
		case MERGE: {
			return new MergeNodeHandlerBuilder(session, (MergeNode) node, this);
		}
		case QUERY:
			return new QueryNodeHandlerBuilder(session, (QueryNode) node, this);
		case NONAME:
			return new NoNameNodeHandlerBuilder(session, (NoNameNode) node, this);
		default:
		}
		throw new RuntimeException("not supported tree node type:" + node.type());
	}

}
