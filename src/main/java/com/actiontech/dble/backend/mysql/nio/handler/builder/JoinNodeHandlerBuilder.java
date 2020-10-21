/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.builder;

import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.backend.mysql.nio.handler.builder.sqlvisitor.PushDownVisitor;
import com.actiontech.dble.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.OrderByHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.TempTableHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.join.JoinHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.join.NotInHandler;
import com.actiontech.dble.backend.mysql.nio.handler.util.CallBackHandler;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.Item.ItemType;
import com.actiontech.dble.plan.common.item.ItemInt;
import com.actiontech.dble.plan.common.item.ItemString;
import com.actiontech.dble.plan.common.item.function.operator.cmpfunc.ItemFuncIn;
import com.actiontech.dble.plan.node.JoinNode;
import com.actiontech.dble.plan.node.PlanNode;
import com.actiontech.dble.plan.util.PlanUtil;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.server.NonBlockingSession;

import java.util.*;

import static com.actiontech.dble.plan.optimizer.JoinStrategyProcessor.NEED_REPLACE;

class JoinNodeHandlerBuilder extends BaseHandlerBuilder {
    private JoinNode node;
    private final int charsertIndex;

    JoinNodeHandlerBuilder(NonBlockingSession session, JoinNode node, HandlerBuilder hBuilder, boolean isExplain) {
        super(session, node, hBuilder, isExplain);
        this.node = node;
        charsertIndex = CharsetUtil.getCollationIndex(session.getSource().getCharsetName().getCollation());
    }

    @Override
    public boolean canDoAsMerge() {
        return PlanUtil.isGlobalOrER(node);
    }

    @Override
    protected void handleSubQueries() {
        handleBlockingSubQuery();
    }

    @Override
    public void mergeBuild() {
        try {
            this.needWhereHandler = false;
            this.canPushDown = !node.existUnPushDownGroup();
            PushDownVisitor pdVisitor = new PushDownVisitor(node, true);
            MergeBuilder mergeBuilder = new MergeBuilder(session, node, pdVisitor);
            String sql = null;
            Map<String, String> mapTableToSimple = new HashMap<>();
            if (node.getAst() != null && node.getParent() == null) { // it's root
                pdVisitor.visit();
                sql = pdVisitor.getSql().toString();
                mapTableToSimple = pdVisitor.getMapTableToSimple();
            }
            SchemaConfig schemaConfig;
            String schemaName = this.session.getShardingService().getSchema();
            if (schemaName != null) {
                schemaConfig = schemaConfigMap.get(schemaName);
            } else {
                schemaConfig = schemaConfigMap.entrySet().iterator().next().getValue(); //random schemaConfig
            }
            RouteResultset rrs;
            // maybe some node is view
            if (sql == null) {
                rrs = mergeBuilder.construct(schemaConfig);
            } else {
                rrs = mergeBuilder.constructByStatement(sql, mapTableToSimple, node.getAst(), schemaConfig);
            }
            buildMergeHandler(node, rrs.getNodes());
        } catch (Exception e) {
            throw new MySQLOutPutException(ErrorCode.ER_QUERYHANDLER, "", "join node mergebuild exception! Error:" + e.getMessage(), e);
        }
    }

    @Override
    public List<DMLResponseHandler> buildPre() {
        List<DMLResponseHandler> pres = new ArrayList<>();
        PlanNode left = node.getLeftNode();
        PlanNode right = node.getRightNode();
        if (node.getStrategy() == JoinNode.Strategy.NESTLOOP) {
            final boolean isLeftSmall = left.getNestLoopFilters() == null;
            final PlanNode tnSmall = isLeftSmall ? left : right;
            final PlanNode tnBig = isLeftSmall ? right : left;
            // prepare the column for sending
            List<Item> keySources = isLeftSmall ? node.getLeftKeys() : node.getRightKeys();
            List<Item> keyToPasses = isLeftSmall ? node.getRightKeys() : node.getLeftKeys();
            // just find one key as filter later, try to choose a simple column(FIELD_ITEM) from toPasses
            int columnIndex = 0;
            for (int index = 0; index < keyToPasses.size(); index++) {
                Item keyToPass = keyToPasses.get(index);
                if (keyToPass.type().equals(ItemType.FIELD_ITEM)) {
                    columnIndex = index;
                    break;
                }
            }
            final Item keySource = keySources.get(columnIndex);
            final Item keyToPass = keyToPasses.get(columnIndex);
            DMLResponseHandler endHandler = buildJoinChild(tnSmall, isLeftSmall);
            final TempTableHandler tempHandler = new TempTableHandler(getSequenceId(), session, keySource);
            endHandler.setNextHandler(tempHandler);
            tempHandler.setLeft(isLeftSmall);
            pres.add(tempHandler);
            CallBackHandler tempDone = new CallBackHandler() {

                @Override
                public void call() throws Exception {
                    Set<String> valueSet = tempHandler.getValueSet();
                    buildNestFilters(tnBig, keyToPass, valueSet, tempHandler.getMaxPartSize());
                    DMLResponseHandler bigLh = buildJoinChild(tnBig, !isLeftSmall);
                    synchronized (tempHandler) {
                        bigLh.setNextHandlerOnly(tempHandler.getNextHandler());
                    }
                    tempHandler.setCreatedHandler(bigLh);
                    HandlerBuilder.startHandler(bigLh);
                }
            };
            if (isExplain) {
                buildNestFiltersForExplain(tnBig, keyToPass);
                DMLResponseHandler bigLh = buildJoinChild(tnBig, !isLeftSmall);
                tempHandler.setCreatedHandler(bigLh);
            }
            tempHandler.setTempDoneCallBack(tempDone);

        } else if (node.getStrategy() == JoinNode.Strategy.SORTMERGE) {
            DMLResponseHandler lh = buildJoinChild(left, true);
            pres.add(lh);
            DMLResponseHandler rh = buildJoinChild(right, false);
            pres.add(rh);

        } else {
            throw new MySQLOutPutException(ErrorCode.ER_QUERYHANDLER, "", "strategy [" + node.getStrategy() + "] not implement yet!");
        }
        return pres;
    }

    private DMLResponseHandler buildJoinChild(PlanNode child, boolean isLeft) {
        BaseHandlerBuilder builder = hBuilder.getBuilder(session, child, isExplain);
        if (builder.getSubQueryBuilderList().size() > 0) {
            this.getSubQueryBuilderList().addAll(builder.getSubQueryBuilderList());
        }
        DMLResponseHandler endHandler = builder.getEndHandler();
        if (isLeft) {
            if (!node.isLeftOrderMatch()) {
                OrderByHandler oh = new OrderByHandler(getSequenceId(), session, node.getLeftJoinOnOrders());
                endHandler.setNextHandler(oh);
                endHandler = oh;
            }
            endHandler.setLeft(true);
        } else {
            if (!node.isRightOrderMatch()) {
                OrderByHandler oh = new OrderByHandler(getSequenceId(), session, node.getRightJoinOnOrders());
                endHandler.setNextHandler(oh);
                endHandler = oh;
            }
        }
        return endHandler;
    }

    @Override
    public void buildOwn() {
        if (node.isNotIn()) {
            NotInHandler nh = new NotInHandler(getSequenceId(), session, node.getLeftJoinOnOrders(),
                    node.getRightJoinOnOrders());
            addHandler(nh);
        } else {
            JoinHandler jh = new JoinHandler(getSequenceId(), session, node.isLeftOuterJoin(),
                    node.getLeftJoinOnOrders(), node.getRightJoinOnOrders(), node.getOtherJoinOnFilter());
            addHandler(jh);
        }
    }

    private void buildNestFiltersForExplain(PlanNode tnBig, Item keyToPass) {
        Item keyInBig = PlanUtil.pushDownItem(node, keyToPass);
        List<Item> strategyFilters = tnBig.getNestLoopFilters();
        List<Item> argList = new ArrayList<>();
        argList.add(keyInBig);
        argList.add(new ItemString(NEED_REPLACE, charsertIndex));
        ItemFuncIn filter = new ItemFuncIn(argList, false, charsertIndex);
        strategyFilters.add(filter);
    }

    /**
     * generate filter for big table according to tmp(small) table's result
     *
     */
    private void buildNestFilters(PlanNode tnBig, Item keyToPass, Set<String> valueSet, int maxPartSize) {
        List<Item> strategyFilters = tnBig.getNestLoopFilters();
        List<Item> partList = null;
        Item keyInBig = PlanUtil.pushDownItem(node, keyToPass);
        int partSize = 0;
        for (String value : valueSet) {
            if (partList == null)
                partList = new ArrayList<>();
            if (value != null) {
                // is null will never join
                partList.add(new ItemString(value, charsertIndex));
                if (++partSize >= maxPartSize) {
                    List<Item> argList = new ArrayList<>();
                    argList.add(keyInBig);
                    argList.addAll(partList);
                    ItemFuncIn inFilter = new ItemFuncIn(argList, false, charsertIndex);
                    strategyFilters.add(inFilter);
                    partList = null;
                    partSize = 0;
                }
            }
        }
        if (partSize > 0) {
            List<Item> argList = new ArrayList<>();
            argList.add(keyInBig);
            argList.addAll(partList);
            ItemFuncIn inFilter = new ItemFuncIn(argList, false, charsertIndex);
            strategyFilters.add(inFilter);
        }
        // if no data
        if (strategyFilters.isEmpty()) {
            strategyFilters.add(new ItemInt(0));
        }
    }

}
