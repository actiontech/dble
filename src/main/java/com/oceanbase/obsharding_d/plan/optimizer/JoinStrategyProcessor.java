/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.optimizer;

import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.oceanbase.obsharding_d.plan.common.exception.MySQLOutPutException;
import com.oceanbase.obsharding_d.plan.node.JoinNode;
import com.oceanbase.obsharding_d.plan.node.PlanNode;
import com.oceanbase.obsharding_d.plan.util.PlanUtil;
import com.oceanbase.obsharding_d.singleton.TraceManager;
import com.google.common.collect.ImmutableMap;

import java.util.List;

public final class JoinStrategyProcessor {
    public static final String NEED_REPLACE = "{NEED_TO_REPLACE}";

    private JoinStrategyProcessor() {
    }

    public static PlanNode optimize(PlanNode qtn, boolean always) {
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("optimize-for-nest-loop");
        try {
            if (PlanUtil.isGlobalOrER(qtn))
                return qtn;

            if (qtn instanceof JoinNode) {
                JoinStrategyChooser chooser = new JoinStrategyChooser((JoinNode) qtn);
                chooser.tryNestLoop(always);
            }
            List<PlanNode> children = qtn.getChildren();
            for (PlanNode child : children) {
                optimize(child, always);
            }
            return qtn;
        } finally {
            TraceManager.log(ImmutableMap.of("plan-node", qtn), traceObject);
            TraceManager.finishSpan(traceObject);
        }
    }

    public static void chooser(PlanNode node) {
        int joinStrategyType = SystemConfig.getInstance().getJoinStrategyType();
        switch (joinStrategyType) {
            case -1:
                if (SystemConfig.getInstance().isUseJoinStrategy()) {
                    optimize(node, false);
                }
                break;
            case 0:
                break;
            case 1:
                optimize(node, false);
                break;
            case 2:
                optimize(node, true);
                break;
            default:
                throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", " joinStrategyType = " + joinStrategyType + " is illegal, size must not be less than -1 and not be greater than 2");

        }
    }
}
