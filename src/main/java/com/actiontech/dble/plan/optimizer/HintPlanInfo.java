/*
 * Copyright (C) 2016-2022 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.plan.optimizer;

import org.jetbrains.annotations.NotNull;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author collapsar
 */
public final class HintPlanInfo implements Iterable<HintPlanNode> {

    private List<HintPlanNodeGroup> groups;
    private boolean left2inner = false;
    private boolean right2inner = false;
    private boolean in2join = false;

    public HintPlanInfo(@Nonnull List<HintPlanNodeGroup> groups) {
        this.groups = groups;
    }

    public HintPlanInfo() {
        groups = new ArrayList<>();
    }

    @Nonnull
    public List<HintPlanNodeGroup> getGroups() {
        return groups;
    }

    public boolean isLeft2inner() {
        return left2inner;
    }

    public boolean isIn2join() {
        return in2join;
    }

    public void setLeft2inner(boolean isLeft2inner) {
        this.left2inner = isLeft2inner;
    }

    public void setIn2join(boolean in2join) {
        this.in2join = in2join;
    }

    public boolean isRight2inner() {
        return right2inner;
    }

    public void setRight2inner(boolean right2inner) {
        this.right2inner = right2inner;
    }

    /**
     * return true if no node exists or hint is not set.
     * @return
     */
    public boolean isZeroNode() {
        return groups.size() == 0 ? true : nodeSize() == 0;
    }


    public long nodeSize() {
        return groups.stream().map(HintPlanNodeGroup::getNodes).mapToLong(List::size).sum();
    }

    @Override
    public String toString() {
        return "HintPlanInfo{" +
                "groups=" + groups +
                ", left2inner=" + left2inner +
                ", in2join=" + in2join +
                '}';
    }

    @NotNull
    @Override
    public Iterator<HintPlanNode> iterator() {
        return new GroupIterator();
    }

    private class GroupIterator implements Iterator<HintPlanNode> {
        int groupIndex = 0;
        int innerIndex = 0;

        @Override
        public boolean hasNext() {
            while (groups.size() - 1 >= groupIndex) {
                final HintPlanNodeGroup group = groups.get(groupIndex);
                final List<HintPlanNode> nodes = group.getNodes();
                if (nodes.size() - 1 >= innerIndex) {
                    return true;
                } else {
                    groupIndex++;
                    innerIndex = 0;
                }
            }
            return false;
        }

        @Override
        public HintPlanNode next() {
            final HintPlanNode node = groups.get(groupIndex).getNodes().get(innerIndex);
            innerIndex++;
            return node;
        }
    }
}
