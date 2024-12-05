/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.dump.handler;

import com.oceanbase.obsharding_d.route.parser.util.Pair;
import com.oceanbase.obsharding_d.services.manager.dump.DumpFileContext;
import com.oceanbase.obsharding_d.services.manager.dump.parse.InsertQueryPos;
import com.alibaba.druid.sql.ast.SQLExpr;

import java.sql.SQLNonTransientException;
import java.util.List;

public class DefaultValuesHandler {


    public void process(DumpFileContext context, InsertQueryPos insertQueryPos, List<Pair<Integer, Integer>> valuePair) throws SQLNonTransientException {

    }

    protected String toString(List<SQLExpr> values, boolean isFirst) {
        StringBuilder sbValues = new StringBuilder(400);
        if (!isFirst) {
            sbValues.append(',');
        }
        sbValues.append('(');
        for (int i = 0; i < values.size(); i++) {
            if (i != 0) {
                sbValues.append(',');
            }
            sbValues.append(values.get(i).toString());
        }
        sbValues.append(')');
        return sbValues.toString();
    }
}
