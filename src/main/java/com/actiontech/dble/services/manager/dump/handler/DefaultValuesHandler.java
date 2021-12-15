package com.actiontech.dble.services.manager.dump.handler;

import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.services.manager.dump.DumpFileContext;
import com.actiontech.dble.services.manager.dump.parse.InsertQueryPos;
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
