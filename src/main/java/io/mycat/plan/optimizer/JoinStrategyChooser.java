package io.mycat.plan.optimizer;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.node.JoinNode;
import io.mycat.plan.node.JoinNode.Strategy;
import io.mycat.plan.node.TableNode;

import java.util.ArrayList;

public class JoinStrategyChooser {
    private JoinNode jn;

    public JoinStrategyChooser(JoinNode jn) {
        this.jn = jn;
    }

    /**
     * 尝试减少join数据
     *
     * @param jn
     * @return boolean true:join优化成功，无须再对join的子节点进行
     * false:join优化失败，需要继续对join的子节点进行尝试
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
     * @param jn
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
     * @param jn
     * @return
     */
    private boolean tryLeftJoinNestLoop() {
        TableNode tnLeft = (TableNode) jn.getLeftNode();
        TableNode tnRight = (TableNode) jn.getRightNode();
        // left join时，只有左有过滤条件，而右没有过滤条件时才适用优化
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
     * 是否是小表，目前一旦有where条件之后就认为是小表
     *
     * @param tn
     * @return
     */
    private boolean isSmallTable(TableNode tn) {
        return tn.getWhereFilter() != null;
    }
}
