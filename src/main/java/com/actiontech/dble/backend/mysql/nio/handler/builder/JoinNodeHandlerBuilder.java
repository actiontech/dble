/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.builder;

import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.backend.mysql.nio.handler.builder.sqlvisitor.PushDownVisitor;
import com.actiontech.dble.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.DelayTableHandler;
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
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.google.common.collect.Lists;
import org.jetbrains.annotations.NotNull;

import java.util.*;

import static com.actiontech.dble.plan.optimizer.JoinStrategyProcessor.NEED_REPLACE;

class JoinNodeHandlerBuilder extends BaseHandlerBuilder {
    private final JoinNode node;
    private final int charsetIndex;
    private boolean optimizerMerge = false;


    JoinNodeHandlerBuilder(NonBlockingSession session, JoinNode node, HandlerBuilder hBuilder, boolean isExplain) {
        super(session, node, hBuilder, isExplain);
        this.node = node;
        charsetIndex = CharsetUtil.getCollationIndex(session.getSource().getService().getCharset().getCollation());
    }

    @Override
    public boolean canDoAsMerge() {
        return PlanUtil.isGlobalOrER(node);
    }

    @Override
    public void mergeBuild(RouteResultset rrs) {
        try {
            buildMergeHandler(node, rrs.getNodes());
        } catch (Exception e) {
            throw new MySQLOutPutException(ErrorCode.ER_QUERYHANDLER, "", "join node mergebuild exception! Error:" + e.getMessage(), e);
        }
    }

    @Override
    protected RouteResultset tryMergeBuild() {
        try {
            this.needWhereHandler = false;
            this.canPushDown = !node.existUnPushDownGroup();
            PushDownVisitor pdVisitor = new PushDownVisitor(node, true);
            RouteResultset rrs = pdVisitor.buildRouteResultset();
            SchemaConfig schemaConfig;
            String schemaName = this.session.getShardingService().getSchema();
            if (schemaName != null) {
                schemaConfig = schemaConfigMap.get(schemaName);
            } else {
                schemaConfig = schemaConfigMap.entrySet().iterator().next().getValue(); //random schemaConfig
            }

            MergeBuilder mergeBuilder = new MergeBuilder(session, node);
            // maybe some node is view
            if (node.getAst() != null && node.getParent() == null) { // it's root
                rrs = mergeBuilder.constructByStatement(rrs, node.getAst(), schemaConfig);
            } else {
                SQLStatementParser parser = new MySqlStatementParser(rrs.getSrcStatement());
                SQLSelectStatement select = (SQLSelectStatement) parser.parseStatement();
                return mergeBuilder.constructByStatement(rrs, select, schemaConfig);
            }
            return rrs;
        } catch (Exception e) {
            throw new MySQLOutPutException(ErrorCode.ER_QUERYHANDLER, "", "join node mergebuild exception! Error:" + e.getMessage(), e);
        }
    }

    @Override
    protected boolean tryBuildWithCurrentNode(List<DMLResponseHandler> subQueryEndHandlers, Set<String> subQueryRouteNodes) {
        HandlerBuilder builder = new HandlerBuilder(node, session);
        //  use node.copy(),it will replace sub-queries to 'NEED_REPLACE'
        List<DMLResponseHandler> merges = Lists.newArrayList();
        {
            BaseHandlerBuilder baseBuilder = builder.getBuilder(session, node.getLeftNode().copy(), isExplain);
            merges.addAll(baseBuilder.getEndHandler().getMerges());
        }
        {
            BaseHandlerBuilder baseBuilder = builder.getBuilder(session, node.getRightNode().copy(), isExplain);
            merges.addAll(baseBuilder.getEndHandler().getMerges());
        }
        Set<String> routeNodes = HandlerBuilder.canRouteToNodes(merges);
        if (routeNodes != null && routeNodes.size() > 0) {
            Set<String> queryRouteNodes = tryRouteWithCurrentNode(subQueryRouteNodes, routeNodes.iterator().next(), routeNodes);
            if (queryRouteNodes.size() >= 1) {
                buildMergeHandlerWithSubQueries(subQueryEndHandlers, queryRouteNodes);
                return true;
            }
        }
        return false;
    }

    @Override
    public List<DMLResponseHandler> buildPre() {
        List<DMLResponseHandler> pres = new ArrayList<>();
        PlanNode left = node.getLeftNode();
        PlanNode right = node.getRightNode();
        if (node.getStrategy() == JoinNode.Strategy.NESTLOOP) {
            if (node.getSubQueries().size() != 0) {
                List<DMLResponseHandler> subQueryEndHandlers;
                subQueryEndHandlers = getSubQueriesEndHandlers(node.getSubQueries());
                if (!isExplain) {
                    // execute subquery sync
                    executeSubQueries(subQueryEndHandlers);
                }
            }
            final boolean isLeftSmall = left.getNestLoopFilters() == null;
            final PlanNode tnSmall = isLeftSmall ? left : right;
            final PlanNode tnBig = isLeftSmall ? right : left;
            // prepare the column for sending
            List<Item> keySources = isLeftSmall ? node.getLeftKeys() : node.getRightKeys();
            List<Item> keyToPasses = isLeftSmall ? node.getRightKeys() : node.getLeftKeys();
            // just find one key as filter later, try to choose a simple column(FIELD_ITEM) from toPasses
            int columnIndex = getColumnIndex(keyToPasses);
            final Item keySource = keySources.get(columnIndex);
            final Item keyToPass = keyToPasses.get(columnIndex);
            DMLResponseHandler endHandler = buildJoinChild(tnSmall, isLeftSmall);
            final TempTableHandler tempHandler = new TempTableHandler(getSequenceId(), session, keySource);
            endHandler.setNextHandler(tempHandler);
            tempHandler.setLeft(isLeftSmall);
            pres.add(tempHandler);
            CallBackHandler tempDone = getCallBackHandler(isLeftSmall, tnBig, keyToPass, tempHandler);
            if (isExplain) {
                buildNestFiltersForExplain(tnBig, keyToPass);
                DMLResponseHandler bigLh = buildJoinChild(tnBig, !isLeftSmall);
                tempHandler.setCreatedHandler(bigLh);
            }
            tempHandler.setTempDoneCallBack(tempDone);

        } else if (node.getStrategy() == JoinNode.Strategy.SORTMERGE) {
            try {
                if (handleSubQueries()) {
                    needWhereHandler = false;
                    optimizerMerge = true;
                    return null;
                }
            } catch (Exception e) {
                throw new MySQLOutPutException(ErrorCode.ER_QUERYHANDLER, "", "JoinNode buildOwn exception! Error:" + e.getMessage(), e);
            }

            DMLResponseHandler lh = buildJoinChild(getDelayTableHandlerMap(), left, true);
            pres.add(lh);
            DMLResponseHandler rh = buildJoinChild(getDelayTableHandlerMap(), right, false);
            pres.add(rh);

            if (tryRouteToOneNode(pres)) {
                pres.clear();
                optimizerMerge = true;
            }
        } else if (node.getStrategy() == JoinNode.Strategy.HINT_NEST_LOOP) {
            if (node.getSubQueries().size() != 0) {
                List<DMLResponseHandler> subQueryEndHandlers;
                subQueryEndHandlers = getSubQueriesEndHandlers(node.getSubQueries());
                if (!isExplain) {
                    // execute subquery sync
                    executeSubQueries(subQueryEndHandlers);
                }
            }
            PlanNode dependNode = right.getNestLoopDependNode();
            boolean isLeftSmall = true;
            if (Objects.isNull(dependNode)) {
                isLeftSmall = false;
            }
            PlanNode tnSmall = left;
            PlanNode tnBig = right;
            List<Item> keySources = node.getLeftKeys();
            List<Item> keyToPasses = node.getRightKeys();
            if (!isLeftSmall) {
                tnSmall = right;
                tnBig = left;
                keySources = node.getRightKeys();
                keyToPasses = node.getLeftKeys();
                dependNode = left.getNestLoopDependNode();
            }
            // prepare the column for sending
            // just find one key as filter later, try to choose a simple column(FIELD_ITEM) from toPasses
            int columnIndex = getColumnIndex(keyToPasses);
            final Item keySource = keySources.get(columnIndex);
            final Item keyToPass = keyToPasses.get(columnIndex);
            DelayTableHandler delayTableHandler = buildDelayHandler(isLeftSmall, tnBig, keySource, keyToPass);
            Map<PlanNode, List<DelayTableHandler>> delayTableHandlerMap = getDelayTableHandlerMap();
            List<DelayTableHandler> delayTableHandlerList = Optional.ofNullable(delayTableHandlerMap.get(dependNode)).orElse(new ArrayList<>());
            delayTableHandlerList.add(delayTableHandler);
            delayTableHandlerMap.put(dependNode, delayTableHandlerList);
            DMLResponseHandler endHandler = buildJoinChild(delayTableHandlerMap, tnSmall, isLeftSmall);
            pres.add(endHandler);
            pres.add(delayTableHandler);
            if (isExplain) {
                buildExplain(isLeftSmall, tnBig, keyToPass, delayTableHandler);
                pres.add(delayTableHandler.getCreatedHandler());
            }
        } else {
            throw new MySQLOutPutException(ErrorCode.ER_QUERYHANDLER, "", "strategy [" + node.getStrategy() + "] not implement yet!");
        }
        return pres;
    }

    @NotNull
    private CallBackHandler getCallBackHandler(boolean isLeftSmall, PlanNode tnBig, Item keyToPass, TempTableHandler tempHandler) {
        CallBackHandler tempDone = () -> {
            Set<String> valueSet = tempHandler.getValueSet();
            buildNestFilters(tnBig, keyToPass, valueSet, tempHandler.getMaxPartSize());
            DMLResponseHandler bigLh = buildJoinChild(tnBig, !isLeftSmall);
            synchronized (tempHandler) {
                bigLh.setNextHandlerOnly(tempHandler.getNextHandler());
            }
            tempHandler.setCreatedHandler(bigLh);
            HandlerBuilder.startHandler(bigLh);
        };
        return tempDone;
    }

    private int getColumnIndex(List<Item> keyToPasses) {
        int columnIndex = 0;
        for (int index = 0; index < keyToPasses.size(); index++) {
            Item keyToPass = keyToPasses.get(index);
            if (keyToPass.type().equals(ItemType.FIELD_ITEM)) {
                columnIndex = index;
                break;
            }
        }
        return columnIndex;
    }

    private DMLResponseHandler buildJoinChild(PlanNode child, boolean isLeft) {
        return buildJoinChild(null, child, isLeft);
    }

    private DMLResponseHandler buildJoinChild(Map<PlanNode, List<DelayTableHandler>> delayTableHandlerMap, PlanNode child, boolean isLeft) {
        BaseHandlerBuilder builder = hBuilder.getBuilder(session, delayTableHandlerMap, child, isExplain);
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

    private DelayTableHandler buildDelayHandler(boolean isLeftSmall, PlanNode tnBig, Item keySource, Item keyToPass) {
        final DelayTableHandler delayTableHandler = new DelayTableHandler(getSequenceId(), session, keySource);
        delayTableHandler.setLeft(!isLeftSmall);
        CallBackHandler tempDone = () -> {
            Set<String> valueSet = delayTableHandler.getValueSet();
            buildNestFilters(tnBig, keyToPass, valueSet, delayTableHandler.getMaxPartSize());
            DMLResponseHandler bigLh = buildJoinChild(getDelayTableHandlerMap(), tnBig, !isLeftSmall);
            bigLh.setNextHandlerOnly(delayTableHandler.getNextHandler());
            delayTableHandler.setCreatedHandler(bigLh);
            HandlerBuilder.startHandler(bigLh);
        };
        delayTableHandler.setTempDoneCallBack(tempDone);
        return delayTableHandler;
    }

    private void buildExplain(boolean isLeftSmall, PlanNode tnBig, Item keyToPass, DelayTableHandler delayTableHandler) {
        buildNestFiltersForExplain(tnBig, keyToPass);
        DMLResponseHandler bigLh = buildJoinChild(getDelayTableHandlerMap(), tnBig, !isLeftSmall);
        delayTableHandler.setCreatedHandler(bigLh);
    }

    @Override
    public void buildOwn() {
        if (optimizerMerge) {
            return;
        }
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
        argList.add(new ItemString(NEED_REPLACE, charsetIndex));
        ItemFuncIn filter = new ItemFuncIn(argList, false, charsetIndex);
        strategyFilters.add(filter);
    }

    /**
     * generate filter for big table according to tmp(small) table's result
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
                partList.add(new ItemString(value, charsetIndex));
                if (++partSize >= maxPartSize) {
                    List<Item> argList = new ArrayList<>();
                    argList.add(keyInBig);
                    argList.addAll(partList);
                    ItemFuncIn inFilter = new ItemFuncIn(argList, false, charsetIndex);
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
            ItemFuncIn inFilter = new ItemFuncIn(argList, false, charsetIndex);
            strategyFilters.add(inFilter);
        }
        // if no data
        if (strategyFilters.isEmpty()) {
            strategyFilters.add(new ItemInt(0));
        }
    }
}
