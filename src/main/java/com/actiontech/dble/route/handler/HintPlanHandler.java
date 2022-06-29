/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.handler;


import com.actiontech.dble.plan.optimizer.HintPlanInfo;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.parser.util.DruidUtil;
import com.actiontech.dble.server.parser.HintPlanParse;
import com.alibaba.druid.sql.ast.SQLStatement;

import java.sql.SQLSyntaxErrorException;

/**
 * sql hint: dble:plan= (a,b,c)&b&(c,d)<br/>
 *
 * @author collapsar
 */
public final class HintPlanHandler {

    private HintPlanHandler() {
    }

    public static RouteResultset route(String hintSQL, int sqlType, String realSql) throws SQLSyntaxErrorException {
        HintPlanParse hintPlanParse = new HintPlanParse();
        hintPlanParse.parse(hintSQL);
        String[] attr = hintSQL.split("\\$");
        HintPlanInfo planInfo = new HintPlanInfo(hintPlanParse.getDependMap(), hintPlanParse.getErMap(), hintPlanParse.getHintPlanNodeMap());
        if (attr.length > 1) {
            for (int i = 1; i < attr.length; i++) {
                switch (attr[i].toLowerCase()) {
                    case "left2inner":
                        planInfo.setLeft2inner(true);
                        break;
                    case "right2inner":
                        planInfo.setRight2inner(true);
                        break;
                    case "in2join":
                        planInfo.setIn2join(true);
                        break;
                    default:
                        break;
                }
            }
        }
        RouteResultset rrs = new RouteResultset(realSql, sqlType);
        rrs.setHintPlanInfo(planInfo);
        SQLStatement statement = DruidUtil.parseSQL(realSql);
        rrs.setNeedOptimizer(true);
        rrs.setSqlStatement(statement);
        return rrs;
    }

}
