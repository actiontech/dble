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
     *
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

    public Iterator<Location> specialIterator() {
        return new GroupIterator2();
    }

    public static class Location {
        public HintPlanNode hintPlanNode;
        public boolean left = true;

        public Location(HintPlanNode hintPlanNode, boolean left) {
            this.hintPlanNode = hintPlanNode;
            this.left = left;
        }
    }

    private class GroupIterator2 implements Iterator<Location> {
        int groupIndex = 0;
        int initGroupIndex = -1;
        Iterator<HintPlanNode> nodeIterator;
        private boolean left = false;


        public GroupIterator2() {
            for (int i = 0; i < groups.size(); i++) {
                final HintPlanNodeGroup group = groups.get(i);
                if (group.getType().equals(HintPlanNodeGroup.Type.ER)) {
                    initGroupIndex = groupIndex = i;
                    nodeIterator = groups.get(groupIndex).getNodes().iterator();
                    left = true;

                }
            }
        }

        @Override
        public boolean hasNext() {
            if (!left) {
                do {
                    if (nodeIterator != null && nodeIterator.hasNext()) {
                        return true;
                    } else {
                        groupIndex++;
                        if (groups.size() - 1 < groupIndex || groupIndex < 0) {
                            break;
                        }
                        nodeIterator = groups.get(groupIndex).getNodes().iterator();
                    }
                } while (true);
            } else {
                do {
                    if (nodeIterator != null && nodeIterator.hasNext()) {
                        return true;
                    } else {
                        groupIndex--;
                        if (groups.size() - 1 < groupIndex || groupIndex < 0) {
                            break;
                        }
                        nodeIterator = groups.get(groupIndex).getNodes().iterator();
                    }
                } while (true);
                groupIndex = initGroupIndex + 1;
                left=false;
                if (groups.size() - 1 < groupIndex || groupIndex < 0) {
                    return false;
                }
                nodeIterator = groups.get(groupIndex).getNodes().iterator();

                do {
                    if (nodeIterator != null && nodeIterator.hasNext()) {
                        return true;
                    } else {
                        groupIndex++;

                        if (groups.size() - 1 < groupIndex || groupIndex < 0) {
                            break;
                        }
                        nodeIterator = groups.get(groupIndex).getNodes().iterator();
                    }
                } while (true);

            }

            return false;
        }

        @Override
        public Location next() {
            return new Location(nodeIterator.next(), left);
        }
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
