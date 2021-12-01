/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.optimizer;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.sharding.table.ERTable;
import com.actiontech.dble.plan.NamedField;
import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.ItemField;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.item.function.operator.cmpfunc.ItemFuncEqual;
import com.actiontech.dble.plan.node.JoinNode;
import com.actiontech.dble.plan.node.PlanNode;
import com.actiontech.dble.plan.node.TableNode;
import com.actiontech.dble.plan.util.FilterUtils;
import com.actiontech.dble.plan.util.PlanUtil;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.util.StringUtil;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.Map.Entry;

public class JoinChooser {
    private static final Logger LOGGER = LogManager.getLogger(JoinChooser.class);
    private final List<JoinRelations> joinRelations = new LinkedList<>();
    private final List<PlanNode> joinUnits = new ArrayList<>();
    private final Map<PlanNode, JoinRelationDag> dagNodes = new HashMap<>();
    private final Map<ERTable, Set<ERTable>> erRelations;
    private final int charsetIndex;
    private final JoinNode orgNode;
    private final List<Item> otherJoinOns = new ArrayList<>();
    private final LinkedList<HintNode> hintNodes;
    private boolean stopOptimize = false;
    private final Comparator<JoinRelationDag> defaultCmp = (o1, o2) -> {
        if (o1.relations.erRelationLst.size() > 0 && o2.relations.erRelationLst.size() > 0) {
            if (o1.relations.isInner) { // both er，o1 inner
                return -1;
            } else if (o2.relations.isInner) { //both er，o1 not inner,o2 inner
                return 1;
            } else { // both er， left join
                return 0;
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
                if (o1.relations.isInner) { //  o1 inner
                    return -1;
                } else if (o2.relations.isInner) { //o1 not inner,o2 inner
                    return 1;
                } else {
                    return 0;
                }
            } else if (o1Global) {
                return -1;
            } else // if (o2Global) {
                return 1;
        }
    };

    public JoinChooser(JoinNode qtn, Map<ERTable, Set<ERTable>> erRelations, LinkedList<HintNode> hintNodes) {
        this.orgNode = qtn;
        this.erRelations = erRelations;
        this.charsetIndex = orgNode.getCharsetIndex();
        this.hintNodes = hintNodes;
    }

    public JoinChooser(JoinNode qtn, LinkedList<HintNode> hintNodes) {
        this(qtn, DbleServer.getInstance().getConfig().getErRelations(), hintNodes);
    }

    public JoinNode optimize() {
        if (erRelations == null) {
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
        initNodeRelations(orgNode);
        JoinNode relationJoin = null;
        if (joinRelations.size() > 0) {
            //make DAG
            JoinRelationDag root = initJoinRelationDag();
            if (stopOptimize) {
                if (!hintNodes.isEmpty()) {
                    //todo :msg:
                    throw new IllegalStateException("Cartesian with relation");
                } else {
                    return orgNode;
                }
            }
            leftCartesianNodes();

            //if custom ,check plan can Follow the rules：Topological Sorting of dag, CartesianNodes
            if (!hintNodes.isEmpty()) {
                relationJoin = joinWithHint(root);
            } else {
                // use auto plan
                relationJoin = makeBNFJoin(root, defaultCmp);
            }
        }
        // no relation join
        if (relationJoin == null) {
            return orgNode;
        }

        // others' node is the join units which can not optimize, just merge them
        JoinNode ret = makeJoinWithCartesianNode(relationJoin);
        ret.setOrderBys(orgNode.getOrderBys());
        ret.setGroupBys(orgNode.getGroupBys());
        ret.select(orgNode.getColumnsSelected());
        ret.setLimitFrom(orgNode.getLimitFrom());
        ret.setLimitTo(orgNode.getLimitTo());
        ret.setOtherJoinOnFilter(orgNode.getOtherJoinOnFilter());
        ret.having(orgNode.getHavingFilter());
        //ret.setWhereFilter(orgNode.getWhereFilter());
        ret.setWhereFilter(FilterUtils.and(orgNode.getWhereFilter(), FilterUtils.and(otherJoinOns)));
        ret.setAlias(orgNode.getAlias());
        ret.setWithSubQuery(orgNode.isWithSubQuery());
        ret.setContainsSubQuery(orgNode.isContainsSubQuery());
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
            if (stopOptimize) {
                return root;
            }
        }
        return root;
    }

    @Nonnull
    private JoinRelationDag createFirstNode() {
        JoinRelations firstRelation = joinRelations.get(0);
        // firstRelation should only have one left nodes
        assert firstRelation.leftNodes.size() == 1;
        JoinRelationDag root = createDag(firstRelation.leftNodes.iterator().next());
        JoinRelationDag right = createDag(firstRelation, firstRelation.isInner);
        root.rightNodes.add(right);
        right.degree++;
        right.leftNodes.add(root);
        return root;
    }


    private boolean isSameNode(HintNode hintNode, JoinRelationDag dagNode) {
        final PlanNode node = dagNode.node;
        if (hintNode.getName() == null) {
            return false;
        }
        return Objects.equals(hintNode.getName(), getUnitName(node));


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

    /**
     * todo: change error msg & add log
     *
     * @param root
     * @return
     */
    private JoinNode joinWithHint(JoinRelationDag root) {
        if (dagNodes.size() != hintNodes.size()) {
            throw new IllegalStateException("hint size not equals to plan node size.");
        }
        if (root.degree != 0) {
            throw new IllegalStateException("any node route to root");
        }
        HintNode nextHintNode = hintNodes.removeFirst();
        JoinNode joinNode = null;
        {
            root = findNode(root, nextHintNode);
            if (root == null) {
                throw new IllegalStateException("no node match the root");
            }
            if (!root.isFamilyInner) {
                throw new IllegalStateException("wrong root node:" + root);
            }
            flipRightNodeToLeft(root, null);
        }
        Set<DagLine> couldAccessDagNodes = new HashSet<>();
        couldAccessDagNodes.add(new DagLine(null, root));


        traversal:
        while (!couldAccessDagNodes.isEmpty()) {
            final Iterator<DagLine> it = couldAccessDagNodes.iterator();
            while (it.hasNext()) {
                final DagLine couldAccessDagNode = it.next();
                final JoinRelationDag currentNode = couldAccessDagNode.getTargetNode();
                final JoinRelationDag prevNode = couldAccessDagNode.getPrevNode();
                if (currentNode.visited) {
                    it.remove();
                    continue;
                }
                /*
                flip may cause node degree increased.
                 */
                if (currentNode.degree != 0) {
                    // LOGGER.warn("degree is changed");
                    it.remove();
                    continue;
                }
                if (!isSameNode(nextHintNode, currentNode)) {
                    continue;
                }

                currentNode.markVisited();
                //matched
                if (prevNode != null) {
                    joinNode = makeJoinNode(prevNode, joinNode, currentNode);
                }

                //prepare for next traversal.
                it.remove();


                if (hintNodes.isEmpty()) {
                    for (DagLine accessDagNode : couldAccessDagNodes) {
                        if (!accessDagNode.getTargetNode().visited) {
                            ////In theory it won't happen
                            throw new IllegalStateException("can't traversal all node use this hint.");
                        }
                    }
                    break traversal;
                }
                nextHintNode = hintNodes.removeFirst();
                for (JoinRelationDag rightNode : currentNode.rightNodes) {
                    --rightNode.degree;
                    //flip only if needed. Unnecessary flip may cause degree num of other nodes increased and become inaccessible.
                    if (rightNode.degree != 0 && isSameNode(nextHintNode, rightNode)) {
                        final boolean flipSuccess = flipRightNodeToLeft(rightNode, currentNode);
                        if (!flipSuccess) {
                            throw new IllegalStateException("can't use this hints,because there is an left join near " + rightNode);
                        }
                        if (rightNode.degree != 0) {
                            //In theory it won't happen
                            throw new IllegalStateException("can't parse use this hints");
                        }
                    }
                    if (rightNode.degree == 0) {
                        couldAccessDagNodes.add(new DagLine(currentNode, rightNode));
                    }

                }
                continue traversal;
            }
            throw new IllegalStateException("can't create plan with wrong hint");

        }

        return joinNode;
    }

    private JoinRelationDag findNode(JoinRelationDag root, HintNode hintNode) {
        if (isSameNode(hintNode, root)) {
            return root;
        } else {
            for (JoinRelationDag rightNode : root.rightNodes) {
                final JoinRelationDag node = findNode(rightNode, hintNode);
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
            it.remove();
            //currentNode move to new left.
            currentNode.rightNodes.add(oldLeft);
            currentNode.degree--;
            //old left move to right.
            oldLeft.degree++;
            oldLeft.leftNodes.add(currentNode);
            oldLeft.rightNodes.remove(currentNode);

            for (OneToOneJoinRelation splitRelation : splitRelations) {
                if (splitRelation.rightNode == oldLeft.node) {
                    oldLeft.relations = addRelations(oldLeft.relations, splitRelation);

                    if (currentNode.relations != null) {
                        subRelation(currentNode.relations, splitRelation);
                    }
                    break;
                }
            }

        }
        return true;
    }

    private JoinNode makeBNFJoin(JoinRelationDag root, Comparator<JoinRelationDag> joinCmp) {

        JoinNode joinNode = null;
        Queue<JoinRelationDag> queue = new LinkedList<>();
        queue.offer(root);
        while (!queue.isEmpty()) {
            JoinRelationDag left = queue.poll();
            List<JoinRelationDag> nextZeroDegreeList = new ArrayList<>();
            for (JoinRelationDag tree : left.rightNodes) {
                if (--tree.degree == 0) {
                    nextZeroDegreeList.add(tree);
                }
            }
            if (nextZeroDegreeList.size() > 0) {
                nextZeroDegreeList.sort(joinCmp);
                for (JoinRelationDag rightNode : nextZeroDegreeList) {
                    joinNode = makeJoinNode(left, joinNode, rightNode);
                    queue.offer(rightNode);
                }
            }
        }
        return joinNode;
    }

    private JoinNode makeJoinNode(JoinRelationDag left, JoinNode joinNode, JoinRelationDag rightNodeOfJoin) {
        boolean leftIsJoin = joinNode != null;
        PlanNode leftNode = leftIsJoin ? joinNode : left.node;
        PlanNode rightNode = rightNodeOfJoin.node;
        joinNode = new JoinNode(leftNode, rightNode, charsetIndex);
        if (!rightNodeOfJoin.relations.isInner) {
            joinNode.setLeftOuterJoin();
        }
        List<ItemFuncEqual> filters = new ArrayList<>();
        for (JoinRelation joinRelation : rightNodeOfJoin.relations.erRelationLst) {
            filters.add(joinRelation.filter);
            if (!leftIsJoin) {
                joinNode.getERkeys().add(joinRelation.left.erTable);
            } else {
                joinNode.getERkeys().addAll(((JoinNode) leftNode).getERkeys());
            }
        }
        for (JoinRelation joinRelation : rightNodeOfJoin.relations.normalRelationLst) {
            filters.add(joinRelation.filter);
        }
        joinNode.setJoinFilter(filters);
        joinNode.setOtherJoinOnFilter(rightNodeOfJoin.relations.otherFilter);
        return joinNode;
    }

    private JoinRelationDag addNodeToDag(JoinRelationDag root, JoinRelations relations) {
        Set<JoinRelationDag> prefixDagNodes = new HashSet<>(relations.prefixNodes.size());
        boolean familyInner = relations.isInner;
        for (PlanNode prefixNode : relations.prefixNodes) {
            JoinRelationDag tmp = dagNodes.get(prefixNode);
            if (tmp == null) {
                // eg: select b.* from  a inner join  b on a.id=b.id , sharding2 inner join   sharding2_child  on sharding2_child.id=sharding2.id ;
                // maybe multi DAGs, or need merge DAGs, optimizer cost too much
                //todo: log
                stopOptimize = true;
                return root;
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
        a.erRelationLst.removeIf(joinRelation -> joinRelation.right.planNode == b.leftNode && joinRelation.left.planNode == b.rightNode);
        a.normalRelationLst.removeIf(joinRelation -> joinRelation.right.planNode == b.leftNode && joinRelation.left.planNode == b.rightNode);
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
        PlanNode orgRightNode = relations.rightNode;
        PlanNode leftNode = orgRightNode;

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
            OneToOneJoinRelation nodeRelations = new OneToOneJoinRelation(tmpErRelationLst, tmpNormalRelationLst, joinRelation.right.planNode, leftNode);
            relationLst.add(nodeRelations);
        }
        for (Entry<PlanNode, List<JoinRelation>> entry : nodeToNormalMap.entrySet()) {
            OneToOneJoinRelation nodeRelations = new OneToOneJoinRelation(new ArrayList<>(0), entry.getValue(), entry.getKey(), leftNode);
            relationLst.add(nodeRelations);
        }
        return relationLst;
    }

    // find the smallest join units in node
    private void initJoinUnits(JoinNode node) {
        for (int index = 0; index < node.getChildren().size(); index++) {
            PlanNode child = node.getChildren().get(index);
            if (isUnit(child)) {
                child = JoinProcessor.optimize(child, hintNodes);
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

    private void initNodeRelations(JoinNode joinNode) {
        for (PlanNode unit : joinUnits) {
            // is unit
            if (unit == joinNode) {
                return;
            }
        }
        for (PlanNode child : joinNode.getChildren()) {
            if ((!isUnit(child)) && (child.type().equals(PlanNode.PlanNodeType.JOIN))) {
                initNodeRelations((JoinNode) child);
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
                    //  if (isERJoin) {
                    //      //todo:  try optimizer later ?leave it even inner join?
                    //  }
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
                otherJoinOns.add(otherFilter);
            } else {
                Set<PlanNode> leftNodes = new HashSet<>();
                getLeftNodes(joinNode.getLeftNode(), leftNodes);
                JoinRelations nodeRelations = new JoinRelations(new ArrayList<>(0), new ArrayList<>(0), otherFilter, rightNode, leftNodes);
                nodeRelations.init();
                joinRelations.add(nodeRelations);
            }
        }
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


    private ERTable getERKey(PlanNode tn, Item c) {
        if (!(c instanceof ItemField))
            return null;
        if (tn.type() != PlanNode.PlanNodeType.TABLE && !PlanUtil.isERNode(tn)) {
            return null;
        }
        Pair<TableNode, ItemField> pair = PlanUtil.findColumnInTableLeaf((ItemField) c, tn);
        if (pair == null)
            return null;
        TableNode tableNode = pair.getKey();
        ItemField col = pair.getValue();
        ERTable erTable = new ERTable(tableNode.getSchema(), tableNode.getPureName(), col.getItemName());
        if (tn.type() == PlanNode.PlanNodeType.TABLE) {
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
                if (DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
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

    private static final class JoinRelationDag {
        private final PlanNode node;
        private int degree = 0;
        private JoinRelations relations;
        private final Set<JoinRelationDag> rightNodes = new HashSet<>();
        private final Set<JoinRelationDag> leftNodes = new HashSet<>();
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
        public String toString() {
            return "{" +
                    "node=" + getUnitName(node) +
                    ", degree=" + degree +
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
    }

    private class JoinRelation {
        private JoinColumnInfo left;
        private JoinColumnInfo right;
        private ItemFuncEqual filter;

        JoinRelation(JoinColumnInfo left, JoinColumnInfo right, ItemFuncEqual filter) {
            this.left = left;
            this.right = right;
            this.filter = filter;
        }

        private JoinRelation exchange() {
            return new JoinRelation(this.right, this.left, FilterUtils.equal(right.key, left.key, charsetIndex));
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

    static final class HintNode {
        String name;

        private HintNode(String name) {
            this.name = name;
        }

        public static HintNode of(String name) {
            return new HintNode(name);
        }

        public String getName() {
            return name;
        }

        @Override
        public String toString() {
            return "name='" + name + '\'';
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

        private static final int HASH_CONST = 37;

        //only calculate the target node
        @Override
        public int hashCode() {
            int hash = 17;
            if (targetNode == null) {
                hash += HASH_CONST;
            } else {
                hash = hash << 5 + hash << 1 + hash + targetNode.hashCode();
            }
            return hash;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof DagLine)) return false;
            DagLine pair = (DagLine) o;
            return Objects.equals(targetNode, pair.targetNode);
        }


    }

}
