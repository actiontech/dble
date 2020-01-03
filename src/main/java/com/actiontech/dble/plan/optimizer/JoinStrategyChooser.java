/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.optimizer;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.node.JoinNode;
import com.actiontech.dble.plan.node.JoinNode.Strategy;
import com.actiontech.dble.plan.node.TableNode;

import java.util.ArrayList;

public class JoinStrategyChooser {
    private JoinNode jn;

    public JoinStrategyChooser(JoinNode jn) {
        this.jn = jn;
    }

    /**
     * tryNestLoop
     *
     * @return boolean true:join can use the nest loop optimization,will not try to optimizer join's child
     * false:join can't use the nest loop optimization,try to optimizer join's child
     */
    public boolean tryNestLoop() {
        if (jn.isNotIn()) {
            return false;
        }
        if (jn.getJoinFilter().isEmpty())
            return false;
        if (jn.isInnerJoin()) {
            return tryInnerJoinNestLoop();
        } else if (jn.getLeftOuter()) {
            return tryLeftJoinNestLoop();
        } else {
            return false;
        }
    }

    /**
     * @return
     */
    private boolean tryInnerJoinNestLoop() {
        TableNode tnLeft = (TableNode) jn.getLeftNode();
        TableNode tnRight = (TableNode) jn.getRightNode();
        boolean isLeftSmall = isSmallTable(tnLeft);
        boolean isRightSmall = isSmallTable(tnRight);
        if (isLeftSmall && isRightSmall)
            return false;
        else if (!isLeftSmall && !isRightSmall)
            return false;
        else {
            handleNestLoopStrategy(isLeftSmall);
            return true;
        }
    }

    /**
     * @return
     */
    private boolean tryLeftJoinNestLoop() {
        TableNode tnLeft = (TableNode) jn.getLeftNode();
        TableNode tnRight = (TableNode) jn.getRightNode();
        // left join and only left node has where filter
        if (isSmallTable(tnLeft) && !isSmallTable(tnRight)) {
            handleNestLoopStrategy(true);
            return true;
        } else {
            return false;
        }
    }

    private void handleNestLoopStrategy(boolean isLeftSmall) {
        jn.setStrategy(Strategy.NESTLOOP);
        TableNode tnLeft = (TableNode) jn.getLeftNode();
        TableNode tnRight = (TableNode) jn.getRightNode();
        TableNode tnBig = isLeftSmall ? tnRight : tnLeft;
        tnBig.setNestLoopFilters(new ArrayList<Item>());
    }

    /**
     * the table contains where is small table now
     *
     * @param tn
     * @return
     */
    private boolean isSmallTable(TableNode tn) {
        return tn.getWhereFilter() != null;
    }
}
