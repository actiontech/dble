/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.optimizer;

import com.actiontech.dble.plan.PlanNode;
import com.actiontech.dble.plan.PlanNode.PlanNodeType;
import com.actiontech.dble.plan.node.JoinNode;
import com.actiontech.dble.plan.util.PlanUtil;

import java.util.HashSet;
import java.util.Set;

public final class GlobalTableProcessor {
    private GlobalTableProcessor() {
    }

    public static PlanNode optimize(PlanNode qtn) {
        initGlobalStatus(qtn);
        return qtn;
    }

    /**
     * @param tn
     * @return true:parent node maybe is global;false parent node must not be global
     */
    private static boolean initGlobalStatus(PlanNode tn) {
        if (tn == null || tn.type() == PlanNodeType.TABLE)
            return true;
        boolean status = true;

        for (PlanNode child : tn.getChildren()) {
            boolean ret = initGlobalStatus(child);
            if (status) {
                status = ret;
            }
        }
        if (PlanUtil.isERNode(tn)) {
            // treat erjoin as an unglobaltable
            tn.setUnGlobalTableCount(1);
            Set<String> newSet = new HashSet<>();
            newSet.addAll(tn.getReferedTableNodes().get(0).getNoshardNode());
            tn.setNoshardNode(newSet);
        } else {
            int unGlobalCount = calcUnGlobalCount(tn);
            tn.setUnGlobalTableCount(unGlobalCount);
            if (tn.getNoshardNode() != null && tn.getNoshardNode().size() == 0) {
                tn.setNoshardNode(null);
            }
            if (!status) {
                tn.setNoshardNode(null);
                return false;
            }
            if (unGlobalCount == 1 && tn instanceof JoinNode) { // joinNode
                JoinNode jn = (JoinNode) tn;
                if (jn.isNotIn()) {
                    tn.setNoshardNode(null);
                    status = false;
                } else if (jn.isInnerJoin()) {
                    if (!isGlobalTableBigEnough(jn)) {
                        tn.setNoshardNode(null);
                        status = false;
                    }
                } else {
                    // left join
                    PlanNode left = jn.getLeftNode();
                    if (left.getUnGlobalTableCount() == 0) { // left node is global,left join will not push down
                        tn.setNoshardNode(null);
                        status = false;
                    } else if (left.type() == PlanNode.PlanNodeType.TABLE || !PlanUtil.isERNode(left)) {
                        if (!isGlobalTableBigEnough(jn)) {
                            tn.setNoshardNode(null);
                            status = false;
                        }
                    } else {
                        // left node is not single table or er table
                        tn.setNoshardNode(null);
                        status = false;
                    }
                }
            } else if (unGlobalCount != 0) {
                tn.setNoshardNode(null);
                status = false;
            }
        }
        return status;
    }

    private static int calcUnGlobalCount(PlanNode tn) {
        int unGlobalCount = 0;
        for (PlanNode tnChild : tn.getChildren()) {
            if (tnChild != null) {
                if (tn.getNoshardNode() == null) {
                    if (tnChild.getNoshardNode() != null) {
                        Set<String> parentSet = new HashSet<>();
                        parentSet.addAll(tnChild.getNoshardNode());
                        tn.setNoshardNode(parentSet);
                    }
                }
                if (tn.getNoshardNode() != null) {
                    tn.getNoshardNode().retainAll(tnChild.getNoshardNode());
                }
                unGlobalCount += tnChild.getUnGlobalTableCount();
            }
        }
        return unGlobalCount;
    }

    private static boolean isGlobalTableBigEnough(JoinNode jn) {
        PlanNode left = jn.getLeftNode();
        PlanNode right = jn.getRightNode();
        PlanNode global, noraml;
        if (left.getUnGlobalTableCount() == 0) {
            global = left;
            noraml = right;
        } else {
            global = right;
            noraml = left;
        }
        Set<String> result = new HashSet<>();
        result.addAll(global.getNoshardNode());
        Set<String> normalSet = noraml.getNoshardNode();
        result.retainAll(normalSet);
        return result.size() == normalSet.size();
    }
}
