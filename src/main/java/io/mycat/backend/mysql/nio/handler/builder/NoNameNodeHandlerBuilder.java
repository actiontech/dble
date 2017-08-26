package io.mycat.backend.mysql.nio.handler.builder;

import io.mycat.backend.mysql.nio.handler.builder.sqlvisitor.PushDownVisitor;
import io.mycat.backend.mysql.nio.handler.query.DMLResponseHandler;
import io.mycat.backend.mysql.nio.handler.query.impl.MultiNodeMergeHandler;
import io.mycat.config.model.SchemaConfig;
import io.mycat.plan.node.NoNameNode;
import io.mycat.route.RouteResultsetNode;
import io.mycat.server.NonBlockingSession;

import java.util.ArrayList;
import java.util.List;

/**
 * query like "select 1 as name"
 *
 * @author ActionTech
 * @CreateTime 2015/3/23
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
        return new ArrayList<>();
    }

    @Override
    public void buildOwn() {
        PushDownVisitor vistor = new PushDownVisitor(node, true);
        vistor.visit();
        this.canPushDown = true;
        String sql = vistor.getSql().toString();
        String schema = session.getSource().getSchema();
        SchemaConfig schemacfg = config.getSchemas().get(schema);
        RouteResultsetNode[] rrss = getTableSources(schemacfg.getAllDataNodes(), sql);
        hBuilder.checkRRSs(rrss);
        MultiNodeMergeHandler mh = new MultiNodeMergeHandler(getSequenceId(), rrss, session.getSource().isAutocommit() && !session.getSource().isTxstart(),
                session, null);
        addHandler(mh);
    }

}
