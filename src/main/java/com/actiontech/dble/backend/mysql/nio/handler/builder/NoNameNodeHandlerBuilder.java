/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.builder;

import com.actiontech.dble.backend.mysql.nio.handler.builder.sqlvisitor.PushDownVisitor;
import com.actiontech.dble.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.MultiNodeMergeHandler;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.plan.node.NoNameNode;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.parser.ServerParse;

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

    protected NoNameNodeHandlerBuilder(NonBlockingSession session, NoNameNode node, HandlerBuilder hBuilder, boolean isExplain) {
        super(session, node, hBuilder, isExplain);
        this.node = node;
        this.needWhereHandler = false;
        this.needCommon = false;
    }

    @Override
    protected void handleSubQueries() {
        handleBlockingSubQuery();
    }

    @Override
    public List<DMLResponseHandler> buildPre() {
        return new ArrayList<>();
    }

    @Override
    public void buildOwn() {
        PushDownVisitor visitor = new PushDownVisitor(node, true);
        visitor.visit();
        this.canPushDown = true;
        String sql = visitor.getSql().toString();
        String schema = session.getSource().getSchema();
        SchemaConfig schemaConfig = schemaConfigMap.get(schema);
        String randomDatenode = getRandomNode(schemaConfig.getAllDataNodes());
        RouteResultsetNode[] rrss = new RouteResultsetNode[]{new RouteResultsetNode(randomDatenode, ServerParse.SELECT, sql)};
        hBuilder.checkRRSs(rrss);
        MultiNodeMergeHandler mh = new MultiNodeMergeHandler(getSequenceId(), rrss, session.getSource().isAutocommit() && !session.getSource().isTxStart(),
                session, null);
        addHandler(mh);
    }

}
