/*
 * Copyright (C) 2016-2017 ActionTech.
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
