/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.information.builder;

import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.backend.mysql.nio.handler.builder.HandlerBuilder;
import com.actiontech.dble.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.OrderByHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.TempTableHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.join.JoinHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.join.NotInHandler;
import com.actiontech.dble.backend.mysql.nio.handler.util.CallBackHandler;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.Item.ItemType;
import com.actiontech.dble.plan.common.item.ItemInt;
import com.actiontech.dble.plan.common.item.ItemString;
import com.actiontech.dble.plan.common.item.function.operator.cmpfunc.ItemFuncIn;
import com.actiontech.dble.plan.node.JoinNode;
import com.actiontech.dble.plan.node.PlanNode;
import com.actiontech.dble.plan.util.PlanUtil;
import com.actiontech.dble.services.manager.ManagerSession;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

class ManagerJoinNodeHandlerBuilder extends ManagerBaseHandlerBuilder {
    private JoinNode node;

    ManagerJoinNodeHandlerBuilder(ManagerSession session, JoinNode node, ManagerHandlerBuilder hBuilder) {
        super(session, node, hBuilder);
        this.node = node;
    }


    @Override
    protected void handleSubQueries() {
        handleBlockingSubQuery();
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
        ManagerBaseHandlerBuilder builder = hBuilder.getBuilder(session, child);
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
                partList.add(new ItemString(value, CharsetUtil.getCollationIndex(session.getSource().getCharsetName().getCollation())));
                if (++partSize >= maxPartSize) {
                    List<Item> argList = new ArrayList<>();
                    argList.add(keyInBig);
                    argList.addAll(partList);
                    ItemFuncIn inFilter = new ItemFuncIn(argList, false, CharsetUtil.getCollationIndex(session.getSource().getCharsetName().getCollation()));
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
            ItemFuncIn inFilter = new ItemFuncIn(argList, false, CharsetUtil.getCollationIndex(session.getSource().getCharsetName().getCollation()));
            strategyFilters.add(inFilter);
        }
        // if no data
        if (strategyFilters.isEmpty()) {
            strategyFilters.add(new ItemInt(0));
        }
    }

}
