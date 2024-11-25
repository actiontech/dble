/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.optimizer;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.config.model.sharding.table.ERTable;
import com.oceanbase.obsharding_d.plan.NamedField;
import com.oceanbase.obsharding_d.plan.common.exception.MySQLOutPutException;
import com.oceanbase.obsharding_d.plan.common.item.Item;
import com.oceanbase.obsharding_d.plan.common.item.ItemField;
import com.oceanbase.obsharding_d.plan.common.item.function.ItemFunc;
import com.oceanbase.obsharding_d.plan.common.item.function.operator.cmpfunc.ItemFuncEqual;
import com.oceanbase.obsharding_d.plan.node.JoinNode;
import com.oceanbase.obsharding_d.plan.node.PlanNode;
import com.oceanbase.obsharding_d.plan.node.TableNode;
import com.oceanbase.obsharding_d.plan.util.FilterUtils;
import com.oceanbase.obsharding_d.plan.util.PlanUtil;
import com.oceanbase.obsharding_d.route.parser.util.Pair;
import com.oceanbase.obsharding_d.util.StringUtil;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.Map.Entry;
import java.util.function.Predicate;

public class JoinChooser {
    private static final Logger LOGGER = LogManager.getLogger(JoinChooser.class);
    private final List<JoinRelations> joinRelations = new LinkedList<>();
    private final List<PlanNode> joinUnits = new ArrayList<>();
    private final Map<PlanNode, JoinRelationDag> dagNodes = new HashMap<>();
    private final Map<ERTable, Set<ERTable>> erRelations;
    private final int charsetIndex;
    private final JoinNode orgNode;
    private final List<Item> otherJoinOns = new ArrayList<>();
    @Nonnull
    private final HintPlanInfo hintPlanInfo;
    private final Comparator<JoinRelationDag> defaultCmp = (o1, o2) -> {
        if (o1.relations.erRelationLst.size() > 0 && o2.relations.erRelationLst.size() > 0) {
            if (o1.relations.isInner == o2.relations.isInner) { // both er,both inner or not inner
                return 0;
            } else if (o1.relations.isInner) { // both er,o1 inner,o2  notinner
                return -1;
            } else { //if (o2.relations.isInner) { both er,o1 not inner,o2 inner
                return 1;
            }
        } else if (o1.relations.erRelationLst.size() > 0) { // if o2 is not ER join, o1 is ER join, o1<o2
            return -1;
        } else if (o2.relations.erRelationLst.size() > 0) { // if o1 is not ER join, o2 is ER join, o1>o2
            return 1;
        } else {
            // both o1,o2 are not ER join, global table should be first
            boolean o1Global = o1.node.getUnGlobalTableCount() == 0;
            boolean o2Global = o2.node.getUnGlobalTableCount() == 0;
            if (o1Global == o2Global) {
                if (o1.relations.isInner == o2.relations.isInner) { // both er,both inner or not inner
                    return 0;
                } else if (o1.relations.isInner) { // both er,o1 inner,o2  notinner
                    return -1;
                } else { //if (o2.relations.isInner) { both er,o1 not inner,o2 inner
                    return 1;
                }
            } else if (o1Global) {
                return -1;
            } else { // if (o2Global) {
                return 1;
            }
        }
    };

    public JoinChooser(JoinNode qtn, Map<ERTable, Set<ERTable>> erRelations, @Nonnull HintPlanInfo hintPlanInfo) {
        this.orgNode = qtn;
        this.erRelations = erRelations;
        this.charsetIndex = orgNode.getCharsetIndex();
        this.hintPlanInfo = hintPlanInfo;
    }

    public JoinChooser(JoinNode qtn, @Nonnull HintPlanInfo hintPlanInfo) {
        this(qtn, OBsharding_DServer.getInstance().getConfig().getErRelations(), hintPlanInfo);
    }

    public JoinNode optimize() {
        if (erRelations == null && hintPlanInfo.isZeroNode()) {
            return orgNode;
        }
        return innerJoinOptimizer();
    }


    /**
     * inner join's ER, rebuild inner join's unit
     */
    private JoinNode innerJoinOptimizer() {
        initJoinUnits(orgNode);
        if (joinUnits.size() == 1) {
            return orgNode;
        }
        JoinNode relationJoin = null;
        if (initNodeRelations(orgNode) && joinRelations.size() > 0) {
            //make DAG
            JoinRelationDag root;
            try {
                root = initJoinRelationDag();
            } catch (OptimizeException e) {
                LOGGER.debug("Join order of  sql  doesn't support to be  optimized. Because {}. The sql is [{}]", e.getMessage(), orgNode);
                if (!hintPlanInfo.isZeroNode()) {
                    throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "we don't support optimize this sql use hints, because " + e.getMessage());
                } else {
                    return orgNode;
                }
            }
            leftCartesianNodes();

            //if custom ,check plan can Follow the rules：Topological Sorting of dag, CartesianNodes
            if (!hintPlanInfo.isZeroNode()) {
                relationJoin = joinWithHint(root);
            } else {
                // use auto plan
                relationJoin = makeBNFJoin(root, defaultCmp);
            }
        }
        // no relation join
        if (relationJoin == null) {
            if (!hintPlanInfo.isZeroNode()) {
                throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "we don't support optimize this sql use hints yet. Maybe this sql contains 'multi right join' or 'cartesian with relation' or 'subquery'.");
            }
            return orgNode;
        }

        // others' node is the join units which can not optimize, just merge them
        JoinNode ret = makeJoinWithCartesianNode(relationJoin);
        ret.setDistinct(orgNode.isDistinct());
        ret.setOrderBys(orgNode.getOrderBys());
        ret.setGroupBys(orgNode.getGroupBys());
        ret.select(orgNode.getColumnsSelected());
        ret.setLimitFrom(orgNode.getLimitFrom());
        ret.setLimitTo(orgNode.getLimitTo());
        ret.having(orgNode.getHavingFilter());
        //ret.setWhereFilter(orgNode.getWhereFilter());
        ret.setWhereFilter(FilterUtils.and(orgNode.getWhereFilter(), FilterUtils.and(otherJoinOns)));
        ret.setAlias(orgNode.getAlias());
        ret.setWithSubQuery(orgNode.isWithSubQuery());
        ret.setContainsSubQuery(orgNode.isContainsSubQuery());
        ret.getSubQueries().addAll(orgNode.getSubQueries());
        ret.setSql(orgNode.getSql());
        ret.setUpFields();
        return ret;
    }


    private JoinNode makeJoinWithCartesianNode(JoinNode node) {
        JoinNode left = node;
        for (PlanNode right : joinUnits) {
            left = new JoinNode(left, right, charsetIndex);
        }
        return left;
    }

    private void leftCartesianNodes() {
        if (joinUnits.size() > dagNodes.size()) {
            //Cartesian Product node
            joinUnits.removeIf(dagNodes::containsKey);
        } else {
            joinUnits.clear();
        }
    }

    @Nonnull
    private JoinRelationDag initJoinRelationDag() {
        JoinRelationDag root = createFirstNode();
        for (int i = 1; i < joinRelations.size(); i++) {
            root = addNodeToDag(root, joinRelations.get(i));
        }
        return root;
    }

    @Nonnull
    private JoinRelationDag createFirstNode() {
        JoinRelations firstRelation = joinRelations.get(0);
        // firstRelation should only have one left nodes
        if (firstRelation.prefixNodes.size() != 1) {
            throw new OptimizeException("firstRelation have " + firstRelation.prefixNodes.size() + " prefix node");
        }

        JoinRelationDag root = createDag(firstRelation.leftNodes.iterator().next());
        JoinRelationDag right = createDag(firstRelation, firstRelation.isInner);
        root.rightNodes.add(right);
        right.degree++;
        right.leftNodes.add(root);
        return root;
    }


    private boolean isSameNode(String hintNodeName, JoinRelationDag dagNode) {
        final PlanNode node = dagNode.node;
        if (hintNodeName == null) {
            return false;
        }
        final String unitName = getUnitName(node);
        if (unitName == null) {
            throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "can't optimize this sql, try to set alias for every tables in sql. Related nodes: " + node);
        }
        return Objects.equals(hintNodeName, unitName);


    }

    @Nullable
    private static String getUnitName(PlanNode node) {
        if (node.getAlias() != null) {
            return node.getAlias();
        } else if (node instanceof TableNode) {
            return ((TableNode) node).getTableName();
        }
        return null;

    }

    private JoinNode joinWithHint(JoinRelationDag root) {
        validateHint();
        if (dagNodes.size() != hintPlanInfo.nodeSize()) {
            throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "hint size " + hintPlanInfo.nodeSize() + " not equals to plan node size " + dagNodes.size() + ".");
        }
        if (root.degree != 0) {
            throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "exists any relations route to the root node: " + root);
        }
        final Iterator<String> hintIt = hintPlanInfo.getHintPlanNodeMap().keySet().stream().iterator();
        String nextHintNode = hintIt.next();

        {
            root = findNode(root, nextHintNode);
            if (root == null) {
                throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "no node match the root: " + nextHintNode);
            }
            if (!root.isFamilyInner) {
                throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "can't use '" + root + "' node for root. Because exists some left join relations point to this node. ");
            }
            flipRightNodeToLeft(root, null);
        }
        final JoinNodeBuilder joinNodeBuilder = new JoinNodeBuilder(root.node);
        Map<JoinRelationDag, DagLine> nextAccessDagNodes = new HashMap<>();
        nextAccessDagNodes.put(root, new DagLine(null, root));

        traversal:
        while (!nextAccessDagNodes.isEmpty()) {
            final Iterator<DagLine> it = nextAccessDagNodes.values().iterator();
            while (it.hasNext()) {
                final DagLine couldAccessDagNode = it.next();
                final JoinRelationDag currentNode = couldAccessDagNode.getTargetNode();
                final JoinRelationDag prevNode = couldAccessDagNode.getPrevNode();
                if (currentNode.visited) {
                    it.remove();
                    continue;
                }

                if (!isSameNode(nextHintNode, currentNode)) {
                    continue;
                }
                /*
                two reason
                1.flip may cause node degree increased.
                2.put rightNode ignore the degree.
                 */
                if (currentNode.degree != 0) {
                    //flip only if needed. Unnecessary flip may cause degree num of other nodes increased and become inaccessible.
                    final boolean flipSuccess = flipRightNodeToLeft(currentNode, prevNode);
                    if (!flipSuccess) {
                        throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "can't use this hints,because exists some left join relations point to node: " + currentNode);
                    }
                    if (currentNode.degree != 0) {
                        //In theory it won't happen
                        throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "can't parse use this hints. " + hintPlanInfo);
                    }
                }
                currentNode.markVisited();
                //matched
                if (prevNode != null) {
                    joinNodeBuilder.appendNodeToRight(currentNode);
                }

                //prepare for next traversal.
                it.remove();
                if (!hintIt.hasNext()) {
                    for (JoinRelationDag targetNode : nextAccessDagNodes.keySet()) {
                        if (!targetNode.visited) {
                            ////In theory it won't happen
                            throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "can't traversal all node use this hint." + hintPlanInfo);
                        }
                    }
                    break traversal;
                }
                nextHintNode = hintIt.next();
                for (JoinRelationDag rightNode : currentNode.rightNodes) {
                    if (rightNode.visited) {
                        continue;
                    }
                    --rightNode.degree;

                    /*
                    ignore the degree and delay the degree comparison .because the rightNode may need flip.
                    e.g.
                    select * from a inner join b on a.id=b.id inner join c on b.id=c.id inner join d on c.id=d.id where b.id=1
                    the join order is a->b->c->d and the hint is c->d->b->a.
                    we choose 'c' as root, then dag like this (c->d) & (c->b<-a)
                    when visited c. b is one of rightNodes of c, and the degree of b is 2.
                     */
                    //                    if (rightNode.degree == 0) {
                    nextAccessDagNodes.put(rightNode, new DagLine(currentNode, rightNode));
                    //                    }

                }
                continue traversal;
            }
            //when no nextAccessDagNodes match the nextHintNode
            String finalNextHintNode = nextHintNode;
            final boolean nodeExist = isHintNodeExist(finalNextHintNode);
            if (!nodeExist) {
                throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "You are using wrong hint.The node '" + nextHintNode + "' doesn't exist.");
            } else {
                throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "You are using wrong hint. please check the node '" + nextHintNode + "',there are no previous nodes connect to it.");
            }


        }
        if (hintIt.hasNext()) {
            throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "can't traversal all node use this hint. please check near the node '" + nextHintNode + "',may be contain orphaned node.");
        }
        return joinNodeBuilder.build();
    }


    private boolean isHintNodeExist(String finalNextHintNodeName) {
        return dagNodes.values().stream().anyMatch(node ->
                isSameNode(finalNextHintNodeName, node)
        );
    }

    private void validateHint() {
        if (hintPlanInfo.erRelyOn()) {
            throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "The ER relation in the hint currently only supports when it exists in the headmost of hint.");
        }
    }

    private JoinRelationDag findNode(JoinRelationDag root, String hintNodeName) {
        if (isSameNode(hintNodeName, root)) {
            return root;
        } else {
            for (JoinRelationDag rightNode : root.rightNodes) {
                final JoinRelationDag node = findNode(rightNode, hintNodeName);
                if (node != null) {
                    return node;
                }
            }
            return null;
        }
    }

    private boolean flipRightNodeToLeft(JoinRelationDag currentNode, JoinRelationDag prevNode) {
        final JoinRelations relations = currentNode.relations;
        if (relations == null) {
            return true;
        }
        if (!relations.isInner) {
            return false;
        }
        final List<OneToOneJoinRelation> splitRelations = splitAndExchangeRelations(relations);

        final Iterator<JoinRelationDag> it = currentNode.leftNodes.iterator();
        while (it.hasNext()) {
            final JoinRelationDag oldLeft = it.next();
            if (oldLeft.visited || oldLeft == prevNode) {
                continue;
            }
            if (oldLeft.relations != null && !oldLeft.relations.isInner) {
                throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "some errors near the node '" + getUnitName(oldLeft.node) + "'. Because left join and inner join can't point to same node.");
            }
            it.remove();
            //currentNode move to new left.
            currentNode.rightNodes.add(oldLeft);
            currentNode.degree--;
            //old left move to right.
            oldLeft.degree++;
            oldLeft.leftNodes.add(currentNode);
            oldLeft.rightNodes.remove(currentNode);

            final Iterator<OneToOneJoinRelation> splitIt = splitRelations.iterator();
            while (splitIt.hasNext()) {
                final OneToOneJoinRelation splitRelation = splitIt.next();
                if (splitRelation.rightNode == oldLeft.node) {
                    oldLeft.relations = addRelations(oldLeft.relations, splitRelation);

                    if (currentNode.relations != null) {
                        subRelation(currentNode.relations, splitRelation);
                    }
                    splitIt.remove();
                }
            }

        }
        return true;
    }

    private JoinNode makeBNFJoin(JoinRelationDag root, Comparator<JoinRelationDag> joinCmp) {
        List<JoinRelationDag> zeroDegreeList = new ArrayList<>();
        for (JoinRelationDag tree : root.rightNodes) {
            if (--tree.degree == 0) {
                zeroDegreeList.add(tree);
            }
        }
        final JoinNodeBuilder joinNodeBuilder = new JoinNodeBuilder(root.node);
        while (!zeroDegreeList.isEmpty()) {
            zeroDegreeList.sort(joinCmp);
            // zeroDegreeList contains no er relations
            boolean cleanList = zeroDegreeList.get(0).relations.erRelationLst.size() == 0;
            Iterator<JoinRelationDag> zeroDegreeIterator = zeroDegreeList.iterator();
            List<JoinRelationDag> newZeroDegreeList = new ArrayList<>();
            while (zeroDegreeIterator.hasNext()) {
                JoinRelationDag rightNode = zeroDegreeIterator.next();
                if (cleanList || rightNode.relations.erRelationLst.size() > 0) {
                    zeroDegreeIterator.remove();
                    joinNodeBuilder.appendNodeToRight(rightNode);
                    for (JoinRelationDag tree : rightNode.rightNodes) {
                        if (--tree.degree == 0) {
                            newZeroDegreeList.add(tree);
                        }
                    }
                } else { // not have any er-node, other nodes will sort with new 0 degree nodes
                    break;
                }
            }
            zeroDegreeList.addAll(newZeroDegreeList);
        }
        return joinNodeBuilder.build();
    }


    private JoinRelationDag addNodeToDag(JoinRelationDag root, JoinRelations relations) {
        Set<JoinRelationDag> prefixDagNodes = new HashSet<>(relations.prefixNodes.size());
        boolean familyInner = relations.isInner;
        for (PlanNode prefixNode : relations.prefixNodes) {
            JoinRelationDag tmp = dagNodes.get(prefixNode);
            if (tmp == null) {
                // eg: select b.* from  a inner join  b on a.id=b.id , sharding2 inner join   sharding2_child  on sharding2_child.id=sharding2.id ;
                // maybe multi DAGs, or need merge DAGs, optimizer cost too much
                throw new OptimizeException("this sql contains cartesian with relation");
            } else {
                prefixDagNodes.add(tmp);
                if (familyInner && !tmp.isFamilyInner) {
                    familyInner = false;
                }
            }
        }


        if (!familyInner || prefixDagNodes.size() == 1 || relations.erRelationLst.size() == 0) {
            // 1.left join can not be optimizer
            // 2. familyInner only one prefixDagNode, no need optimizer
            // 3. all join filter are not er, no need change direction
            JoinRelationDag right = createDag(relations, familyInner);
            for (JoinRelationDag prefixDagNode : prefixDagNodes) {
                prefixDagNode.rightNodes.add(right);
                right.degree++;
            }
            right.leftNodes.addAll(prefixDagNodes);
            return root;
        } else {
            Set<PlanNode> toChangeParent = new HashSet<>();
            for (JoinRelation joinRelation : relations.erRelationLst) {
                toChangeParent.add(joinRelation.left.planNode);
            }
            return optimizerInnerJoinOtherFilter(createDag(relations.rightNode), relations, prefixDagNodes, toChangeParent);
        }
    }

    private JoinRelationDag optimizerInnerJoinOtherFilter(JoinRelationDag newLeft, JoinRelations relations, Set<JoinRelationDag> orgLefts, Set<PlanNode> toChangeParent) {
        //if (relations.leftNodes.size() < relations.prefixNodes.size()) {
        //    todo:a inner join b on (ab) inner join c on (bc,ab)
        //}

        //eg: a inner join b on (ab) inner join c on (bc,ac)
        //change direction to:c inner join b on (bc) inner join a on (ab,ac)
        if (orgLefts.size() == 0 && relations == null) { // root and not with new
            return newLeft;
        }
        for (JoinRelationDag orgLeft : orgLefts) {
            if (toChangeParent.contains(orgLeft.node)) {
                // inner join prefixNodes==left nodes
                Set<PlanNode> toChange = orgLeft.relations == null ? null : orgLeft.relations.prefixNodes;
                optimizerInnerJoinOtherFilter(orgLeft, orgLeft.relations, orgLeft.leftNodes, toChange);
            }
        }
        List<OneToOneJoinRelation> splitRelationLst = splitAndExchangeRelations(relations);
        for (OneToOneJoinRelation splitRelation : splitRelationLst) {
            JoinRelationDag oldLeft = null;
            for (JoinRelationDag orgLeft : orgLefts) {
                if (splitRelation.rightNode == orgLeft.node) {
                    oldLeft = orgLeft;
                    break;
                }
            }
            //change direction
            assert oldLeft != null;
            oldLeft.leftNodes.add(newLeft);
            oldLeft.rightNodes.remove(newLeft);
            oldLeft.degree++;
            oldLeft.relations = addRelations(oldLeft.relations, splitRelation);

            if (newLeft.relations != null) {
                subRelation(newLeft.relations, splitRelation);
            }
            newLeft.leftNodes.remove(oldLeft); // root remove nothing
            newLeft.rightNodes.add(oldLeft);
            if (newLeft.degree > 0) { //root is 0
                newLeft.degree--;
            }
        }
        return newLeft;
    }

    private void subRelation(JoinRelations a, OneToOneJoinRelation b) {
        if (a == null) {
            return;
        }
        final Predicate<JoinRelation> predicate = joinRelation -> (joinRelation.right.planNode == b.rightNode && joinRelation.left.planNode == b.leftNode) || (joinRelation.right.planNode == b.leftNode && joinRelation.left.planNode == b.rightNode);
        a.erRelationLst.removeIf(predicate);
        a.normalRelationLst.removeIf(predicate);
        a.init();
    }


    private JoinRelations addRelations(JoinRelations a, OneToOneJoinRelation b) {
        if (a == null) {
            return b.convertToJoinRelations();
        }
        a.erRelationLst.addAll(b.erRelationLst);
        a.normalRelationLst.addAll(b.normalRelationLst);
        a.init();
        return a;
    }

    private List<OneToOneJoinRelation> splitAndExchangeRelations(JoinRelations relations) {
        PlanNode newLeftNode = relations.rightNode;

        List<OneToOneJoinRelation> relationLst = new ArrayList<>();
        Map<PlanNode, List<JoinRelation>> nodeToNormalMap = new HashMap<>();
        for (JoinRelation joinRelation : relations.normalRelationLst) {
            joinRelation = joinRelation.exchange();
            List<JoinRelation> tmpNormalList = nodeToNormalMap.get(joinRelation.right.planNode);
            if (tmpNormalList == null) {
                tmpNormalList = new ArrayList<>();
            }
            tmpNormalList.add(joinRelation);
            nodeToNormalMap.put(joinRelation.right.planNode, tmpNormalList);
        }
        for (JoinRelation joinRelation : relations.erRelationLst) {
            joinRelation = joinRelation.exchange();
            List<JoinRelation> tmpErRelationLst = new ArrayList<>(1);
            tmpErRelationLst.add(joinRelation);
            List<JoinRelation> tmpNormalRelationLst = nodeToNormalMap.remove(joinRelation.right.planNode);
            if (tmpNormalRelationLst == null) {
                tmpNormalRelationLst = new ArrayList<>(0);
            }
            OneToOneJoinRelation nodeRelations = new OneToOneJoinRelation(tmpErRelationLst, tmpNormalRelationLst, joinRelation.right.planNode, newLeftNode);
            relationLst.add(nodeRelations);
        }
        for (Entry<PlanNode, List<JoinRelation>> entry : nodeToNormalMap.entrySet()) {
            OneToOneJoinRelation nodeRelations = new OneToOneJoinRelation(new ArrayList<>(0), entry.getValue(), entry.getKey(), newLeftNode);
            relationLst.add(nodeRelations);
        }
        return relationLst;
    }

    // find the smallest join units in node
    private void initJoinUnits(JoinNode node) {
        for (int index = 0; index < node.getChildren().size(); index++) {
            PlanNode child = node.getChildren().get(index);
            if (isUnit(child)) {
                child = JoinProcessor.optimize(child, new HintPlanInfo());
                node.getChildren().set(index, child);
                this.joinUnits.add(child);
            } else {
                initJoinUnits((JoinNode) child);
            }
        }
    }


    private boolean isUnit(PlanNode node) {
        return node.type() != PlanNode.PlanNodeType.JOIN || node.isWithSubQuery();
    }

    private boolean initNodeRelations(JoinNode joinNode) {
        for (PlanNode unit : joinUnits) {
            // is unit
            if (unit == joinNode) {
                return true;
            }
        }
        PlanNode right = joinNode.getChildren().get(1);
        if ((!isUnit(right)) && (right.type().equals(PlanNode.PlanNodeType.JOIN))) {
            right = JoinProcessor.optimize(right, new HintPlanInfo());
            joinNode.setRightNode(right);
            return false; //will be not optimize current join node
        }

        PlanNode left = joinNode.getChildren().get(0);
        if ((!isUnit(left)) && (left.type().equals(PlanNode.PlanNodeType.JOIN))) {
            if (!initNodeRelations((JoinNode) left)) {
                return false;
            }
        }

        Item otherFilter = joinNode.getOtherJoinOnFilter();
        PlanNode rightNode = joinNode.getRightNode();
        if (joinNode.getJoinFilter().size() > 0) {
            List<JoinRelation> erRelationLst = new ArrayList<>(1);
            List<JoinRelation> normalRelationLst = new ArrayList<>(1);
            Set<PlanNode> leftNodes = new HashSet<>(2);
            for (ItemFuncEqual filter : joinNode.getJoinFilter()) {
                JoinColumnInfo columnInfoLeft = initJoinColumnInfo(filter.arguments().get(0));
                JoinColumnInfo columnInfoRight = initJoinColumnInfo(filter.arguments().get(1));
                boolean isERJoin = isErRelation(columnInfoLeft.erTable, columnInfoRight.erTable);
                if (columnInfoLeft.planNode != rightNode && columnInfoRight.planNode != rightNode) {
                    //  now may not happen:a join b on a,b join c on c,b and a,b; the last a,b can be other filter
                    //todo: if (isERJoin) and inner join try optimizer later
                    otherFilter = FilterUtils.and(otherFilter, filter);
                    continue;
                }
                JoinRelation nodeRelation = new JoinRelation(columnInfoLeft, columnInfoRight, filter);
                if (isERJoin) {
                    erRelationLst.add(nodeRelation);
                } else {
                    normalRelationLst.add(nodeRelation);
                }
                leftNodes.add(columnInfoLeft.planNode);
            }

            JoinRelations nodeRelations;
            if (joinNode.isInnerJoin()) {
                otherJoinOns.add(otherFilter);
                nodeRelations = new JoinRelations(erRelationLst, normalRelationLst, rightNode, leftNodes);
            } else {
                nodeRelations = new JoinRelations(erRelationLst, normalRelationLst, otherFilter, rightNode, leftNodes);
            }
            nodeRelations.init();
            joinRelations.add(nodeRelations);
        } else {
            if (joinNode.isInnerJoin()) {
                if (!hintPlanInfo.isZeroNode()) {
                    throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "we don't support optimize this sql use hints yet. Because this sql contains 'cartesian with relation'.");
                }
                otherJoinOns.add(otherFilter);
            } else {
                Set<PlanNode> leftNodes = new HashSet<>();
                getLeftNodes(joinNode.getLeftNode(), leftNodes);
                JoinRelations nodeRelations = new JoinRelations(new ArrayList<>(0), new ArrayList<>(0), otherFilter, rightNode, leftNodes);
                nodeRelations.init();
                joinRelations.add(nodeRelations);
            }
        }
        return true;
    }

    private void getLeftNodes(PlanNode child, Set<PlanNode> leftNodes) {
        if ((!isUnit(child)) && (child.type().equals(PlanNode.PlanNodeType.JOIN))) {
            getLeftNodes(((JoinNode) child).getLeftNode(), leftNodes);
            getLeftNodes(((JoinNode) child).getRightNode(), leftNodes);
        } else {
            leftNodes.add(child);
        }
    }

    private JoinColumnInfo initJoinColumnInfo(Item key) {
        JoinColumnInfo columnInfo = new JoinColumnInfo(key);
        for (PlanNode planNode : joinUnits) {
            Item tmpSel = nodeHasSelectTable(planNode, columnInfo.key);
            if (tmpSel != null) {
                columnInfo.planNode = planNode;
                columnInfo.erTable = getERKey(planNode, tmpSel);
                return columnInfo;
            }
        }
        throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "can not find table of:" + key);
    }


    private boolean onlyContainsOneTable(PlanNode queryNode) {
        PlanNode child = queryNode.getChildren().get(0);
        if (child.type() == PlanNode.PlanNodeType.TABLE) {
            return true;
        } else if (child.type() == PlanNode.PlanNodeType.QUERY) {
            return onlyContainsOneTable(child);
        } else {
            return false;
        }
    }

    private ERTable getERKey(PlanNode tn, Item column) {
        if (!(column instanceof ItemField))
            return null;
        Pair<TableNode, ItemField> pair = null;
        if (tn.type() == PlanNode.PlanNodeType.QUERY && onlyContainsOneTable(tn)) {
            pair = PlanUtil.findColumnInTableLeaf((ItemField) column, tn);
        } else if (tn.type() != PlanNode.PlanNodeType.TABLE && !PlanUtil.isERNode(tn)) {
            return null;
        } else {
            pair = PlanUtil.findColumnInTableLeaf((ItemField) column, tn);
        }
        if (pair == null)
            return null;
        TableNode tableNode = pair.getKey();
        ItemField col = pair.getValue();
        ERTable erTable = new ERTable(tableNode.getSchema(), tableNode.getPureName(), col.getItemName());
        if (tn.type() == PlanNode.PlanNodeType.TABLE || tn.type() == PlanNode.PlanNodeType.QUERY) {
            return erTable;
        } else {
            List<ERTable> erList = ((JoinNode) tn).getERkeys();
            for (ERTable cerKey : erList) {
                if (isErRelation(erTable, cerKey))
                    return erTable;
            }
            return null;
        }
    }


    private Item nodeHasSelectTable(PlanNode child, Item sel) {
        if (sel instanceof ItemField) {
            return nodeHasColumn(child, (ItemField) sel);
        } else if (sel.canValued()) {
            return sel;
        } else if (sel.type().equals(Item.ItemType.SUM_FUNC_ITEM)) {
            return null;
        } else {
            ItemFunc fcopy = (ItemFunc) sel.cloneStruct();
            for (int index = 0; index < fcopy.getArgCount(); index++) {
                Item arg = fcopy.arguments().get(index);
                Item argSel = nodeHasSelectTable(child, arg);
                if (argSel == null)
                    return null;
                else
                    fcopy.arguments().set(index, argSel);
            }
            PlanUtil.refreshReferTables(fcopy);
            fcopy.setPushDownName(null);
            return fcopy;
        }
    }

    private Item nodeHasColumn(PlanNode child, ItemField col) {
        String colName = col.getItemName();
        if (StringUtil.isEmpty(col.getTableName())) {
            for (Entry<NamedField, Item> entry : child.getOuterFields().entrySet()) {
                if (StringUtil.equalsIgnoreCase(colName, entry.getKey().getName())) {
                    return entry.getValue();
                }
            }
        } else {
            String table = col.getTableName();
            if (child.getAlias() == null) {
                for (Entry<NamedField, Item> entry : child.getOuterFields().entrySet()) {
                    if (StringUtil.equals(table, entry.getKey().getTable()) &&
                            StringUtil.equalsIgnoreCase(colName, entry.getKey().getName())) {
                        return entry.getValue();
                    }
                }
            } else {
                if (OBsharding_DServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
                    if (!StringUtil.equalsIgnoreCase(table, child.getAlias()))
                        return null;
                } else {
                    if (!StringUtil.equals(table, child.getAlias()))
                        return null;
                }
                for (Entry<NamedField, Item> entry : child.getOuterFields().entrySet()) {
                    if (StringUtil.equalsIgnoreCase(colName, entry.getKey().getName())) {
                        return entry.getValue();
                    }
                }
            }
        }
        return null;
    }

    private boolean isErRelation(ERTable er0, ERTable er1) {
        if (er0 == null || er1 == null) {
            return false;
        }
        Set<ERTable> erList = erRelations.get(er0);
        if (erList == null) {
            return false;
        }
        return erList.contains(er1);
    }

    public JoinRelationDag createDag(PlanNode node) {
        JoinRelationDag dagNode = dagNodes.get(node);
        if (dagNode == null) {
            dagNode = new JoinRelationDag(node);
            dagNodes.put(node, dagNode);
        }
        return dagNode;
    }


    public JoinRelationDag createDag(JoinRelations relations, boolean isFamilyInner) {
        JoinRelationDag dagNode = dagNodes.get(relations.rightNode);
        if (dagNode == null) {
            dagNode = new JoinRelationDag(relations, isFamilyInner);
            dagNodes.put(relations.rightNode, dagNode);
        }
        return dagNode;
    }

    private final class JoinNodeBuilder {
        private final PlanNode rootNode;
        private JoinNode result;

        private JoinNodeBuilder(PlanNode rootNode) {
            this.rootNode = rootNode;
        }

        private void appendNodeToRight(JoinRelationDag rightNodeOfJoin) {
            boolean leftIsNull = result == null;
            JoinNode joinNode = new JoinNode(leftIsNull ? rootNode : result, rightNodeOfJoin.node, charsetIndex);
            if (!rightNodeOfJoin.relations.isInner) {
                joinNode.setLeftOuterJoin();
            }
            List<ItemFuncEqual> filters = new ArrayList<>();
            for (JoinRelation joinRelation : rightNodeOfJoin.relations.erRelationLst) {
                filters.add(joinRelation.filter);
                if (leftIsNull) {
                    joinNode.getERkeys().add(joinRelation.left.erTable);
                } else {
                    joinNode.getERkeys().addAll((result).getERkeys());
                }
            }
            for (JoinRelation joinRelation : rightNodeOfJoin.relations.normalRelationLst) {
                filters.add(joinRelation.filter);
            }
            joinNode.setJoinFilter(filters);
            joinNode.setOtherJoinOnFilter(rightNodeOfJoin.relations.otherFilter);
            this.result = joinNode;
        }

        public JoinNode build() {
            return result;
        }
    }

    private static final class JoinRelationDag {
        private final PlanNode node;
        private int degree = 0;
        private JoinRelations relations;
        private final Set<JoinRelationDag> rightNodes = new LinkedHashSet<>();
        private final Set<JoinRelationDag> leftNodes = new LinkedHashSet<>();
        private boolean isFamilyInner = true;
        private boolean visited = false;

        private JoinRelationDag(PlanNode node) {
            this.node = node;
            this.relations = null;
        }

        private JoinRelationDag(JoinRelations relations, boolean isFamilyInner) {
            this.node = relations.rightNode;
            this.relations = relations;
            this.isFamilyInner = isFamilyInner;
        }


        public void markVisited() {
            this.visited = true;
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return super.equals(obj);
        }

        @Override
        public String toString() {
            return "{" +
                    "node=" + getUnitName(node) +
                    '}';
        }
    }


    private class JoinRelations {
        private final List<JoinRelation> erRelationLst;
        private final List<JoinRelation> normalRelationLst;
        private final boolean isInner;
        private final Item otherFilter;
        private final Set<PlanNode> leftNodes;
        private final PlanNode rightNode;
        private final Set<PlanNode> prefixNodes = new HashSet<>();

        JoinRelations(List<JoinRelation> erRelationLst, List<JoinRelation> normalRelationLst, Item otherFilter, PlanNode rightNode, Set<PlanNode> leftNodes) {
            this.erRelationLst = erRelationLst;
            this.normalRelationLst = normalRelationLst;
            this.rightNode = rightNode;
            this.leftNodes = leftNodes;
            this.isInner = false;
            this.otherFilter = otherFilter;
        }

        JoinRelations(List<JoinRelation> erRelationLst, List<JoinRelation> normalRelationLst, PlanNode rightNode, Set<PlanNode> leftNodes) {
            this.erRelationLst = erRelationLst;
            this.normalRelationLst = normalRelationLst;
            this.rightNode = rightNode;
            this.leftNodes = leftNodes;
            this.isInner = true;
            this.otherFilter = null;
        }

        void init() {
            prefixNodes.clear();
            prefixNodes.addAll(leftNodes);
            if (otherFilter != null && otherFilter.getReferTables() != null) {
                for (PlanNode planNode : joinUnits) {
                    if (planNode != rightNode) {
                        Item tmpSel = nodeHasSelectTable(planNode, otherFilter);
                        if (tmpSel != null) {
                            prefixNodes.add(planNode);
                        }
                    }
                }
            }
        }

        @Override
        public String toString() {
            return String.valueOf(Iterables.concat(normalRelationLst, erRelationLst));
        }

    }


    private class OneToOneJoinRelation {
        private final List<JoinRelation> erRelationLst;
        private final List<JoinRelation> normalRelationLst;
        private final PlanNode leftNode;
        private final PlanNode rightNode;


        OneToOneJoinRelation(List<JoinRelation> erRelationLst, List<JoinRelation> normalRelationLst, PlanNode rightNode, PlanNode leftNode) {
            this.erRelationLst = erRelationLst;
            this.normalRelationLst = normalRelationLst;
            this.rightNode = rightNode;
            this.leftNode = leftNode;
        }


        public JoinRelations convertToJoinRelations() {
            final JoinRelations ret = new JoinRelations(erRelationLst, normalRelationLst, rightNode, Sets.newHashSet(leftNode));
            ret.init();
            return ret;
        }

        @Override
        public String toString() {
            return getUnitName(leftNode) + " --> " + getUnitName(rightNode);
        }
    }

    private class JoinRelation {
        private final JoinColumnInfo left;
        private final JoinColumnInfo right;
        private final ItemFuncEqual filter;

        JoinRelation(JoinColumnInfo left, JoinColumnInfo right, ItemFuncEqual filter) {
            this.left = left;
            this.right = right;
            this.filter = filter;
        }

        private JoinRelation exchange() {
            return new JoinRelation(this.right, this.left, FilterUtils.equal(right.key, left.key, charsetIndex));
        }

        @Override
        public String toString() {
            return left + " --> " + right;
        }
    }

    /**
     * JoinColumnInfo
     *
     * @author ActionTech
     */
    private static class JoinColumnInfo {
        private Item key; // join on's on key
        private PlanNode planNode; // treenode of the joinColumn belong to
        private ERTable erTable; //  joinColumn is er ,if so,save th parentkey

        JoinColumnInfo(Item key) {
            this.key = key;
            planNode = null;
            erTable = null;
        }

        @Override
        public int hashCode() {
            int hash = this.key.getTableName().hashCode();
            hash = hash * 31 + this.key.getItemName().toLowerCase().hashCode();
            return hash;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null)
                return false;
            if (o == this)
                return true;
            if (!(o instanceof JoinColumnInfo)) {
                return false;
            }
            JoinColumnInfo other = (JoinColumnInfo) o;
            if (this.key == null)
                return false;
            return StringUtil.equals(this.key.getTableName(), other.key.getTableName()) &&
                    StringUtil.equalsIgnoreCase(this.key.getItemName(), other.key.getItemName());
        }

        @Override
        public String toString() {
            return "key:" + key;
        }

        public Item getKey() {
            return key;
        }

        public void setKey(Item key) {
            this.key = key;
        }

        public PlanNode getPlanNode() {
            return planNode;
        }

        public void setPlanNode(PlanNode planNode) {
            this.planNode = planNode;
        }

        public ERTable getErTable() {
            return erTable;
        }

        public void setErTable(ERTable erTable) {
            this.erTable = erTable;
        }
    }

    public static final class DagLine {

        private final JoinRelationDag prevNode;
        private final JoinRelationDag targetNode;

        public DagLine(JoinRelationDag prevNode, JoinRelationDag targetNode) {
            this.prevNode = prevNode;
            this.targetNode = targetNode;
        }

        public JoinRelationDag getPrevNode() {
            return prevNode;
        }

        public JoinRelationDag getTargetNode() {
            return targetNode;
        }

        @Override
        public String toString() {
            return "(" + prevNode + ", " + targetNode + ")";
        }

    }

    private static class OptimizeException extends RuntimeException {
        OptimizeException(String message) {
            super(message);
        }
    }
}
