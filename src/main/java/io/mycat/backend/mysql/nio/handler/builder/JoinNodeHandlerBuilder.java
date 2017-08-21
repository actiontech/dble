package io.mycat.backend.mysql.nio.handler.builder;

import io.mycat.backend.mysql.nio.handler.builder.sqlvisitor.PushDownVisitor;
import io.mycat.backend.mysql.nio.handler.query.DMLResponseHandler;
import io.mycat.backend.mysql.nio.handler.query.impl.OrderByHandler;
import io.mycat.backend.mysql.nio.handler.query.impl.TempTableHandler;
import io.mycat.backend.mysql.nio.handler.query.impl.join.JoinHandler;
import io.mycat.backend.mysql.nio.handler.query.impl.join.NotInHandler;
import io.mycat.backend.mysql.nio.handler.util.CallBackHandler;
import io.mycat.config.ErrorCode;
import io.mycat.plan.PlanNode;
import io.mycat.plan.common.exception.MySQLOutPutException;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.Item.ItemType;
import io.mycat.plan.common.item.ItemInt;
import io.mycat.plan.common.item.ItemString;
import io.mycat.plan.common.item.function.operator.cmpfunc.ItemFuncIn;
import io.mycat.plan.node.JoinNode;
import io.mycat.plan.util.PlanUtil;
import io.mycat.route.RouteResultsetNode;
import io.mycat.server.NonBlockingSession;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

class JoinNodeHandlerBuilder extends BaseHandlerBuilder {
    private JoinNode node;

    protected JoinNodeHandlerBuilder(NonBlockingSession session, JoinNode node, HandlerBuilder hBuilder) {
        super(session, node, hBuilder);
        this.node = node;
    }

    @Override
    public boolean canDoAsMerge() {
        if (PlanUtil.isGlobalOrER(node))
            return true;
        else
            return false;
    }

    @Override
    public void mergeBuild() {
        try {
            this.needWhereHandler = false;
            this.canPushDown = !node.existUnPushDownGroup();
            PushDownVisitor pdVisitor = new PushDownVisitor(node, true);
            MergeBuilder mergeBuilder = new MergeBuilder(session, node, needCommon, needSendMaker, pdVisitor);
            //TODO:
            RouteResultsetNode[] rrssArray = mergeBuilder.construct().getNodes();
            this.needCommon = mergeBuilder.getNeedCommonFlag();
            this.needSendMaker = mergeBuilder.getNeedSendMakerFlag();
            buildMergeHandler(node, rrssArray);
        } catch (Exception e) {
            throw new MySQLOutPutException(ErrorCode.ER_QUERYHANDLER, "", "join node mergebuild exception!", e);
        }
    }

    @Override
    public List<DMLResponseHandler> buildPre() {
        List<DMLResponseHandler> pres = new ArrayList<DMLResponseHandler>();
        PlanNode left = node.getLeftNode();
        PlanNode right = node.getRightNode();
        if (node.getStrategy() == JoinNode.Strategy.NESTLOOP) {
            final boolean isLeftSmall = left.getNestLoopFilters() == null;
            final PlanNode tnSmall = isLeftSmall ? left : right;
            final PlanNode tnBig = isLeftSmall ? right : left;
            // 确定准备传递的列
            List<Item> keySources = isLeftSmall ? node.getLeftKeys() : node.getRightKeys();
            List<Item> keyToPasses = isLeftSmall ? node.getRightKeys() : node.getLeftKeys();
            // 只需要传递第一个key过滤条件给目标节点就ok， 尽量选择toPass的是Column的
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
                    bigLh.setNextHandler(tempHandler.getNextHandler());
                    tempHandler.setCreatedHandler(bigLh);
                    // 启动handler
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
        DMLResponseHandler endHandler = hBuilder.buildNode(session, child);
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
     * 根据临时表的数据生成新的大表的过滤条件
     *
     * @param tnBig
     * @param keyToPass
     * @param valueSet
     */
    protected void buildNestFilters(PlanNode tnBig, Item keyToPass, Set<String> valueSet, int maxPartSize) {
        List<Item> strategyFilters = tnBig.getNestLoopFilters();
        List<Item> partList = null;
        Item keyInBig = PlanUtil.pushDownItem(node, keyToPass);
        int partSize = 0;
        for (String value : valueSet) {
            if (partList == null)
                partList = new ArrayList<Item>();
            if (value == null) { // is null will never join
                continue;
            } else {
                partList.add(new ItemString(value));
                if (++partSize >= maxPartSize) {
                    List<Item> argList = new ArrayList<Item>();
                    argList.add(keyInBig);
                    argList.addAll(partList);
                    ItemFuncIn inFilter = new ItemFuncIn(argList, false);
                    strategyFilters.add(inFilter);
                    partList = null;
                    partSize = 0;
                }
            }
        }
        if (partSize > 0) {
            List<Item> argList = new ArrayList<Item>();
            argList.add(keyInBig);
            argList.addAll(partList);
            ItemFuncIn inFilter = new ItemFuncIn(argList, false);
            strategyFilters.add(inFilter);
        }
        // 没有数据
        if (strategyFilters.isEmpty()) {
            strategyFilters.add(new ItemInt(0));
        }
    }

}
