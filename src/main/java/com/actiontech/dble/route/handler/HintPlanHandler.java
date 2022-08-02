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
import java.util.List;

/**
 * sql hint: dble:plan= (a,b,c)&b&(c,d)<br/>
 *
 * @author collapsar
 */
public final class HintPlanHandler {

    private HintPlanHandler() {
    }

    public static RouteResultset route(String hintSQL, int sqlType, String realSql) throws SQLSyntaxErrorException {
        RouteResultset rrs = new RouteResultset(realSql, sqlType);
        SQLStatement statement = DruidUtil.parseSQL(realSql);
        HintPlanInfo planInfo = parseHint(hintSQL, statement);
        rrs.setHintPlanInfo(planInfo);
        rrs.setNeedOptimizer(true);
        rrs.setSqlStatement(statement);
        return rrs;
    }

    public static HintPlanInfo parseHint(String hintSQL, SQLStatement statement) {
        String[] attr = hintSQL.split("\\$");
        HintPlanInfo planInfo = new HintPlanInfo();

        String realHint = "";
        boolean useTableIndex = false;
        if (attr.length > 1) {
            for (String s : attr) {
                switch (s.toLowerCase()) {
                    case "left2inner":
                        planInfo.setLeft2inner(true);
                        break;
                    case "right2inner":
                        planInfo.setRight2inner(true);
                        break;
                    case "in2join":
                        planInfo.setIn2join(true);
                        break;
                    case "use_table_index":
                        useTableIndex = true;
                        break;
                    default:
                        realHint = s;
                        break;
                }
            }
        }

        if (useTableIndex && statement != null) {
            realHint = replaceTableIndex(statement, realHint);
        }

        HintPlanParse hintPlanParse = new HintPlanParse();
        hintPlanParse.parse(realHint);
        planInfo.setRelationMap(hintPlanParse);
        return planInfo;
    }

    private static String replaceTableIndex(SQLStatement sqlStatement, String hintSQL) {
        List<String> tableNames = DruidUtil.getTableNamesBySql(sqlStatement);
        String newSql = hintSQL;
        for (int i = 0; i < tableNames.size(); i++) {
            newSql = newSql.replaceAll(i + 1 + "", tableNames.get(i));
        }
        return newSql;
    }

}
