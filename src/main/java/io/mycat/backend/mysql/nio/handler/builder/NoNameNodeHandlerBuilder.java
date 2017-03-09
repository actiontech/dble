package io.mycat.backend.mysql.nio.handler.builder;

import java.util.ArrayList;
import java.util.List;

import io.mycat.backend.mysql.nio.handler.builder.sqlvisitor.PushDownVisitor;
import io.mycat.backend.mysql.nio.handler.query.DMLResponseHandler;
import io.mycat.backend.mysql.nio.handler.query.impl.MultiNodeMergeHandler;
import io.mycat.config.model.SchemaConfig;
import io.mycat.plan.node.NoNameNode;
import io.mycat.route.RouteResultsetNode;
import io.mycat.server.NonBlockingSession;

/**
 * select 1 as name这种sql
 * 
 * @author ActionTech
 * @CreateTime 2015年3月23日
 */
class NoNameNodeHandlerBuilder extends BaseHandlerBuilder {
	private NoNameNode node;

	protected NoNameNodeHandlerBuilder(NonBlockingSession session, NoNameNode node, HandlerBuilder hBuilder) {
		super(session, node, hBuilder);
		this.node = node;
		this.needWhereHandler = false;
		this.needCommon = false;
	}

	@Override
	public List<DMLResponseHandler> buildPre() {
		return new ArrayList<DMLResponseHandler>();
	}

	@Override
	public void buildOwn() {
		PushDownVisitor vistor = new PushDownVisitor(node, true);
		vistor.visit();
		this.canPushDown = true;
		String sql = vistor.getSql().toString();
		String schema = session.getSource().getSchema();
		SchemaConfig schemacfg = mycatConfig.getSchemas().get(schema);
		RouteResultsetNode[] rrss = getTableSources(schemacfg.getAllDataNodes(),sql);
		hBuilder.checkRRSS(rrss);
		MultiNodeMergeHandler mh = new MultiNodeMergeHandler(getSequenceId(), rrss, session.getSource().isAutocommit(),
				session, null);
		addHandler(mh);
	}

}
