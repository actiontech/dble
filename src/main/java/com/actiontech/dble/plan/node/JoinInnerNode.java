package com.actiontech.dble.plan.node;

import com.actiontech.dble.plan.util.ToStringUtil;

/**
 * Created by szf on 2019/5/31.
 */
public class JoinInnerNode extends PlanNode {


    public PlanNodeType type() {
        return PlanNode.PlanNodeType.JOIN_INNER;
    }

    public JoinInnerNode() {
        super();
    }

    public JoinInnerNode(PlanNode left, PlanNode right) {
        super();
        addChild(left);
        addChild(right);
        setKeepFieldSchema(left.isKeepFieldSchema() && right.isKeepFieldSchema());
    }

    public String getPureName() {
        return null;
    }


    @Override
    public String getPureSchema() {
        return null;
    }


    @Override
    public int getHeight() {
        int maxChildHeight = 0;
        for (PlanNode child : children) {
            int childHeight = child.getHeight();
            if (childHeight > maxChildHeight)
                maxChildHeight = childHeight;
        }
        return maxChildHeight + 1;
    }

    @Override
    public PlanNode copy() {
        JoinInnerNode newJoinNode = new JoinInnerNode();
        this.copySelfTo(newJoinNode);
        return newJoinNode;
    }

    @Override
    public String toString(int level) {
        StringBuilder sb = new StringBuilder();
        String tabTittle = ToStringUtil.getTab(level);
        if (this.getAlias() != null) {
            ToStringUtil.appendln(sb, tabTittle + "Join" + " as " + this.getAlias());
        } else {
            ToStringUtil.appendln(sb, tabTittle + "Join");
        }
        return sb.toString();
    }

    public PlanNode getLeftNode() {
        if (children.isEmpty())
            return null;
        return children.get(0);

    }

    public PlanNode getRightNode() {
        if (children.size() < 2)
            return null;
        return children.get(1);
    }

}
