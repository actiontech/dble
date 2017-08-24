package io.mycat.backend.mysql.nio.handler.builder;

import com.alibaba.druid.sql.ast.SQLOrderingSpecification;
import io.mycat.MycatServer;
import io.mycat.backend.mysql.nio.handler.builder.sqlvisitor.GlobalVisitor;
import io.mycat.backend.mysql.nio.handler.query.DMLResponseHandler;
import io.mycat.backend.mysql.nio.handler.query.impl.*;
import io.mycat.backend.mysql.nio.handler.query.impl.groupby.DirectGroupByHandler;
import io.mycat.backend.mysql.nio.handler.query.impl.groupby.OrderedGroupByHandler;
import io.mycat.config.ErrorCode;
import io.mycat.config.MycatConfig;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.TableConfig;
import io.mycat.plan.Order;
import io.mycat.plan.PlanNode;
import io.mycat.plan.PlanNode.PlanNodeType;
import io.mycat.plan.common.exception.MySQLOutPutException;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.sumfunc.ItemSum;
import io.mycat.plan.common.item.function.sumfunc.ItemSum.Sumfunctype;
import io.mycat.plan.node.JoinNode;
import io.mycat.plan.node.QueryNode;
import io.mycat.plan.node.TableNode;
import io.mycat.plan.util.PlanUtil;
import io.mycat.route.RouteResultsetNode;
import io.mycat.server.NonBlockingSession;
import io.mycat.server.parser.ServerParse;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

abstract class BaseHandlerBuilder {
    private static AtomicLong sequenceId = new AtomicLong(0);
    protected NonBlockingSession session;
    protected HandlerBuilder hBuilder;
    protected DMLResponseHandler start;
    /* 当前的最后一个handler */
    protected DMLResponseHandler currentLast;
    private PlanNode node;
    protected MycatConfig config;
    /* 是否可以全下推 */
    protected boolean canPushDown = false;
    /* 是否需要common中的handler，包括group by，order by，limit等 */
    protected boolean needCommon = true;
    /* 是否需要过wherehandler过滤 */
    protected boolean needWhereHandler = true;
    /* 直接从用户的sql下发的是不需要sendmaker */
    protected boolean needSendMaker = true;

    protected BaseHandlerBuilder(NonBlockingSession session, PlanNode node, HandlerBuilder hBuilder) {
        this.session = session;
        this.node = node;
        this.hBuilder = hBuilder;
        this.config = MycatServer.getInstance().getConfig();
        if (config.getSchemas().isEmpty())
            throw new MySQLOutPutException(ErrorCode.ER_QUERYHANDLER, "", "current router config is empty!");
    }

    public DMLResponseHandler getEndHandler() {
        return currentLast;
    }

    /**
     * 生成正确的hanlder链
     */
    public final void build() {
        List<DMLResponseHandler> preHandlers = null;
        // 是否切换了join策略
        boolean joinStrategyed = isNestLoopStrategy(node);
        if (joinStrategyed) {
            nestLoopBuild();
        } else if (!node.isExsitView() && node.getUnGlobalTableCount() == 0 && node.getNoshardNode() != null) {
            // 可确定统一下发到某个节点
            noShardBuild();
        } else if (canDoAsMerge()) {
            //可下发到某些(1...n)结点,如果：eg: ER tables,  GLOBAL*NORMAL GLOBAL*ER
            mergeBuild();
        } else {
            //拆分
            preHandlers = buildPre();
            buildOwn();
        }
        if (needCommon)
            buildCommon();
        if (needSendMaker) {
            // view subalias
            String tbAlias = node.getAlias();
            if (node.getParent() != null && node.getParent().getSubAlias() != null)
                tbAlias = node.getParent().getSubAlias();
            SendMakeHandler sh = new SendMakeHandler(getSequenceId(), session, node.getColumnsSelected(), tbAlias);
            addHandler(sh);
        }
        if (preHandlers != null) {
            for (DMLResponseHandler preHandler : preHandlers) {
                preHandler.setNextHandler(start);
            }
        }
    }

    /**
     * 虽然where和otherjoinOn过滤条件为空，但是存在strategyFilters作为过滤条件
     */
    protected void nestLoopBuild() {
        throw new MySQLOutPutException(ErrorCode.ER_QUERYHANDLER, "", "not implement yet, node type[" + node.type() + "]");
    }

    /* join的er关系，或者global优化以及tablenode，可以当成merge来做 */
    protected boolean canDoAsMerge() {
        return false;
    }

    protected void mergeBuild() {
        //
    }

    protected abstract List<DMLResponseHandler> buildPre();

    /**
     * 生成自己的handler
     */
    protected abstract void buildOwn();

    /**
     * 不存在拆分表，下推到第一个节点
     */
    protected final void noShardBuild() {
        this.needCommon = false;
        // 默认的可以global的都是unglobalcount=0，除了是joinnode有特殊情况
        // 當且僅當node.unglobalcount=0,所以所有的語句都可以下發，僅需要將語句拼出來下發到一個節點即可
        String sql = null;
        if (node.getParent() == null) { // root节点
            sql = node.getSql();
        }
        // 有可能node来自于view
        if (sql == null) {
            GlobalVisitor visitor = new GlobalVisitor(node, true);
            visitor.visit();
            sql = visitor.getSql().toString();
        } else {
            needSendMaker = false;
        }
        RouteResultsetNode[] rrss = getTableSources(node.getNoshardNode(), sql);
        hBuilder.checkRRSs(rrss);
        MultiNodeMergeHandler mh = new MultiNodeMergeHandler(getSequenceId(), rrss, session.getSource().isAutocommit() && !session.getSource().isTxstart(),
                session, null);
        addHandler(mh);
    }

    /**
     * 构建node公共的属性，包括where，groupby，having，orderby，limit，还有最后的sendmakehandler
     */
    protected void buildCommon() {
        if (node.getWhereFilter() != null && needWhereHandler) {
            WhereHandler wh = new WhereHandler(getSequenceId(), session, node.getWhereFilter());
            addHandler(wh);
        }
        /* need groupby handler */
        if (nodeHasGroupBy(node)) {
            boolean needOrderBy = (node.getGroupBys().size() > 0) ? isOrderNeeded(node, node.getGroupBys()) : false;
            boolean canDirectGroupBy = true;
            List<ItemSum> sumRefs = new ArrayList<ItemSum>();
            for (ItemSum funRef : node.sumFuncs) {
                if (funRef.hasWithDistinct() || funRef.sumType().equals(Sumfunctype.GROUP_CONCAT_FUNC))
                    canDirectGroupBy = false;
                sumRefs.add(funRef);
            }
            if (needOrderBy) {
                if (canDirectGroupBy) {
                    // we go direct groupby
                    DirectGroupByHandler gh = new DirectGroupByHandler(getSequenceId(), session, node.getGroupBys(),
                            sumRefs);
                    addHandler(gh);
                } else {
                    OrderByHandler oh = new OrderByHandler(getSequenceId(), session, node.getGroupBys());
                    addHandler(oh);
                    OrderedGroupByHandler gh = new OrderedGroupByHandler(getSequenceId(), session, node.getGroupBys(),
                            sumRefs);
                    addHandler(gh);
                }
            } else { // @bug 1052 canDirectGroupby condition we use
                // directgroupby already
                OrderedGroupByHandler gh = new OrderedGroupByHandler(getSequenceId(), session, node.getGroupBys(),
                        sumRefs);
                addHandler(gh);
            }
        }
        // having
        if (node.getHavingFilter() != null) {
            HavingHandler hh = new HavingHandler(getSequenceId(), session, node.getHavingFilter());
            addHandler(hh);
        }

        if (node.isDistinct() && node.getOrderBys().size() > 0) {
            // distinct and order by both exists
            List<Order> mergedOrders = mergeOrderBy(node.getColumnsSelected(), node.getOrderBys());
            if (mergedOrders == null) {
                // can not merge,need distinct then order by
                DistinctHandler dh = new DistinctHandler(getSequenceId(), session, node.getColumnsSelected());
                addHandler(dh);
                OrderByHandler oh = new OrderByHandler(getSequenceId(), session, node.getOrderBys());
                addHandler(oh);
            } else {
                DistinctHandler dh = new DistinctHandler(getSequenceId(), session, node.getColumnsSelected(),
                        mergedOrders);
                addHandler(dh);
            }
        } else {
            if (node.isDistinct()) {
                DistinctHandler dh = new DistinctHandler(getSequenceId(), session, node.getColumnsSelected());
                addHandler(dh);
            }
            // order by
            if (node.getOrderBys().size() > 0) {
                if (node.getGroupBys().size() > 0) {
                    if (!PlanUtil.orderContains(node.getGroupBys(), node.getOrderBys())) {
                        OrderByHandler oh = new OrderByHandler(getSequenceId(), session, node.getOrderBys());
                        addHandler(oh);
                    }
                } else if (isOrderNeeded(node, node.getOrderBys())) {
                    OrderByHandler oh = new OrderByHandler(getSequenceId(), session, node.getOrderBys());
                    addHandler(oh);
                }
            }
        }
        if (node.getLimitTo() > 0) {
            LimitHandler lh = new LimitHandler(getSequenceId(), session, node.getLimitFrom(), node.getLimitTo());
            addHandler(lh);
        }

    }

    /**
     * 添加一个handler到hanlder链
     */
    protected void addHandler(DMLResponseHandler bh) {
        if (currentLast == null) {
            start = bh;
            currentLast = bh;
        } else {
            currentLast.setNextHandler(bh);
            currentLast = bh;
        }
        bh.setAllPushDown(canPushDown);
    }

    /*----------------------------- helper method -------------------*/
    private boolean isNestLoopStrategy(PlanNode node) {
        return node.type() == PlanNodeType.TABLE && node.getNestLoopFilters() != null;
    }

    /**
     * 是否需要对该node进行orderby排序
     * 如果该node的上一层handler返回的结果已经按照orderBys排序，则无需再次进行orderby
     *
     * @param node
     * @param orderBys
     * @return
     */
    private boolean isOrderNeeded(PlanNode node, List<Order> orderBys) {
        if (node instanceof TableNode || PlanUtil.isGlobalOrER(node))
            return false;
        else if (node instanceof JoinNode) {
            return !isJoinNodeOrderMatch((JoinNode) node, orderBys);
        } else if (node instanceof QueryNode) {
            return !isQueryNodeOrderMatch((QueryNode) node, orderBys);
        }
        return true;
    }

    /**
     * joinnode的默认排序记录在leftjoinonorders和rightjoinonorders中
     *
     * @param jn
     * @param orderBys 目标排序
     * @return
     */
    private boolean isJoinNodeOrderMatch(JoinNode jn, List<Order> orderBys) {
        // 记录orderBys中前面出现的onCondition列,如jn.onCond = (t1.id=t2.id),
        // orderBys为t1.id,t2.id,t1.name，则onOrders = {t1.id,t2.id};
        List<Order> onOrders = new ArrayList<Order>();
        List<Order> leftOnOrders = jn.getLeftJoinOnOrders();
        List<Order> rightOnOrders = jn.getRightJoinOnOrders();
        for (Order orderBy : orderBys) {
            if (leftOnOrders.contains(orderBy) || rightOnOrders.contains(orderBy)) {
                onOrders.add(orderBy);
            } else {
                break;
            }
        }
        if (onOrders.isEmpty()) {
            // joinnode的数据一定是按照joinOnCondition进行排序的
            return false;
        } else {
            List<Order> remainOrders = orderBys.subList(onOrders.size(), orderBys.size());
            if (remainOrders.isEmpty()) {
                return true;
            } else {
                List<Order> pushedOrders = PlanUtil.getPushDownOrders(jn, remainOrders);
                if (jn.isLeftOrderMatch()) {
                    List<Order> leftChildOrders = jn.getLeftNode().getOrderBys();
                    List<Order> leftRemainOrders = leftChildOrders.subList(leftOnOrders.size(), leftChildOrders.size());
                    if (PlanUtil.orderContains(leftRemainOrders, pushedOrders))
                        return true;
                } else if (jn.isRightOrderMatch()) {
                    List<Order> rightChildOrders = jn.getRightNode().getOrderBys();
                    List<Order> rightRemainOrders = rightChildOrders.subList(rightOnOrders.size(),
                            rightChildOrders.size());
                    if (PlanUtil.orderContains(rightRemainOrders, pushedOrders))
                        return true;
                }
                return false;
            }
        }
    }

    /**
     * @param qn
     * @param orderBys 目标排序
     * @return
     */
    private boolean isQueryNodeOrderMatch(QueryNode qn, List<Order> orderBys) {
        List<Order> childOrders = qn.getChild().getOrderBys();
        List<Order> pushedOrders = PlanUtil.getPushDownOrders(qn, orderBys);
        return PlanUtil.orderContains(childOrders, pushedOrders);
    }

    /**
     * 尝试将order by的顺序合并到columnsSelected中
     *
     * @param columnsSelected
     * @param orderBys
     * @return
     */
    private List<Order> mergeOrderBy(List<Item> columnsSelected, List<Order> orderBys) {
        List<Integer> orderIndexes = new ArrayList<Integer>();
        List<Order> newOrderByList = new ArrayList<Order>();
        for (Order orderBy : orderBys) {
            Item column = orderBy.getItem();
            int index = columnsSelected.indexOf(column);
            if (index < 0)
                return null;
            else
                orderIndexes.add(index);
            Order newOrderBy = new Order(columnsSelected.get(index), orderBy.getSortOrder());
            newOrderByList.add(newOrderBy);
        }
        for (int index = 0; index < columnsSelected.size(); index++) {
            if (!orderIndexes.contains(index)) {
                Order newOrderBy = new Order(columnsSelected.get(index), SQLOrderingSpecification.ASC);
                newOrderByList.add(newOrderBy);
            }
        }
        return newOrderByList;
    }

    protected static boolean nodeHasGroupBy(PlanNode arg) {
        return (arg.sumFuncs.size() > 0 || arg.getGroupBys().size() > 0);
    }

    protected static long getSequenceId() {
        return sequenceId.incrementAndGet();
    }

    /*-----------------计算datasource相关start------------------*/

    protected void buildMergeHandler(PlanNode node, RouteResultsetNode[] rrssArray) {
        hBuilder.checkRRSs(rrssArray);
        MultiNodeMergeHandler mh = null;
        List<Order> orderBys = node.getGroupBys().size() > 0 ? node.getGroupBys() : node.getOrderBys();

        mh = new MultiNodeMergeHandler(getSequenceId(), rrssArray, session.getSource().isAutocommit() && !session.getSource().isTxstart(), session,
                orderBys);
        addHandler(mh);
    }

    protected RouteResultsetNode[] getTableSources(Set<String> dataNodes, String sql) {
        String randomDatenode = null;
        int index = (int) (System.currentTimeMillis() % dataNodes.size());
        int i = 0;
        for (String dataNode : dataNodes) {
            if (index == i) {
                randomDatenode = dataNode;
                break;
            }
            i++;
        }
        RouteResultsetNode rrss = new RouteResultsetNode(randomDatenode, ServerParse.SELECT, sql);
        return new RouteResultsetNode[]{rrss};
    }

    protected TableConfig getTableConfig(String schema, String table) {
        SchemaConfig schemaConfig = this.config.getSchemas().get(schema);
        if (schemaConfig == null)
            return null;
        return schemaConfig.getTables().get(table);
    }
    /*-------------------------------计算datasource相关end------------------*/

}
