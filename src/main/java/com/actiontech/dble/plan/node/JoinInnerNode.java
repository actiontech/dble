package com.actiontech.dble.plan.node;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.ItemField;
import com.actiontech.dble.plan.util.ToStringUtil;
import com.actiontech.dble.route.parser.druid.RouteTableConfigInfo;

import java.util.List;

/**
 * Created by szf on 2019/5/31.
 */
public class JoinInnerNode extends PlanNode {


    public PlanNodeType type() {
        return PlanNode.PlanNodeType.JOIN_INNER;
    }

    private JoinInnerNode() {
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
    public RouteTableConfigInfo findFieldSourceFromIndex(int index) throws Exception {
        if (columnsSelected.size() > index) {
            Item sourceColumns = columnsSelected.get(index);
            for (PlanNode pn : this.getChildren()) {
                if ((pn.getAlias() != null && pn.getAlias().equals(sourceColumns.getTableName())) ||
                        (pn.getAlias() == null && pn.getPureName().equals(sourceColumns.getTableName()))) {
                    for (int i = 0; i < pn.columnsSelected.size(); i++) {
                        Item cSelected = pn.columnsSelected.get(i);
                        if (cSelected.getAlias() != null && cSelected.getAlias().equals(sourceColumns.getItemName())) {
                            return pn.findFieldSourceFromIndex(i);
                        } else if (cSelected.getAlias() == null && cSelected.getItemName().equals(sourceColumns.getItemName())) {
                            return pn.findFieldSourceFromIndex(i);
                        }
                    }
                }
            }
        }
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


    public PlanNode select(List<Item> columnSelected) {
        for (Item i : columnSelected) {
            if (i.isWild() && i.getTableName() == null && i instanceof ItemField) {
                ((ItemField) i).setTableName(getRightNode().getAlias() == null ? getRightNode().getPureName() : getRightNode().getAlias());
            }
        }
        this.columnsSelected = columnSelected;
        return this;
    }
}
