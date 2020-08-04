/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.builder;

import com.actiontech.dble.backend.mysql.nio.handler.builder.sqlvisitor.PushDownVisitor;
import com.actiontech.dble.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.MultiNodeEasyMergeHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.MultiNodeFakeHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.MultiNodeMergeHandler;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFuncInner;
import com.actiontech.dble.plan.node.MergeNode;
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
        String schema = session.getShardingService().getSchema();
        SchemaConfig schemaConfig = schemaConfigMap.get(schema);
        String randomDatenode = getRandomNode(schemaConfig.getAllShardingNodes());
        RouteResultsetNode[] rrss = new RouteResultsetNode[]{new RouteResultsetNode(randomDatenode, ServerParse.SELECT, sql)};
        hBuilder.checkRRSs(rrss);
        MultiNodeMergeHandler mh = new MultiNodeEasyMergeHandler(getSequenceId(), rrss, session.getShardingService().isAutocommit() && !session.getShardingService().isTxStart(), session);
        addHandler(mh);
    }

    @Override
    protected void noShardBuild() {
        this.needCommon = false;
        //if the node is NoNameNode
        boolean allSelectInnerFunc = true;
        for (Item i : this.node.getColumnsSelected()) {
            if (!(i instanceof ItemFuncInner)) {
                allSelectInnerFunc = false;
                break;
            }
        }
        if (allSelectInnerFunc) {
            boolean union = false;
            if (this.node.getParent() instanceof MergeNode) {
                union = ((MergeNode) this.node.getParent()).isUnion();
            }
            MultiNodeMergeHandler mh = new MultiNodeFakeHandler(getSequenceId(), session, this.node.getColumnsSelected(), union);
            addHandler(mh);
            return;
        }
        super.noShardBuild();

    }

}
