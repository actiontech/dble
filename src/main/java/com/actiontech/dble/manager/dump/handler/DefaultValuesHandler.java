package com.actiontech.dble.manager.dump.handler;

import com.actiontech.dble.manager.dump.DumpFileContext;
import com.actiontech.dble.singleton.SequenceManager;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;

import java.sql.SQLNonTransientException;
import java.util.List;

class DefaultValuesHandler {

//    protected StringBuilder insertHeader;
    

    public void preProcess(DumpFileContext context) throws InterruptedException {
        if (insertHeader == null) {
            return;
        }
        for (String dataNode : context.getTableConfig().getDataNodes()) {
            context.getWriter().write(dataNode, insertHeader.toString(), true, false);
        }
    }

    public void postProcess(DumpFileContext context) throws InterruptedException {
        for (String dataNode : context.getTableConfig().getDataNodes()) {
            context.getWriter().write(dataNode, ";", false, false);
        }
        insertHeader = null;
    }

    public void process(DumpFileContext context, List<SQLExpr> values, boolean isFirst) throws SQLNonTransientException, InterruptedException {
        int incrementIndex = context.getIncrementColumnIndex();
        if (incrementIndex == -1) {
            return;
        }

        String tableKey = StringUtil.getFullName(context.getSchema(), context.getTable());
        long val = SequenceManager.getHandler().nextId(tableKey);
        SQLExpr value = values.get(incrementIndex);
        if (!StringUtil.isEmpty(SQLUtils.toMySqlString(value)) && !context.isNeedSkipError()) {
            context.addError("For table using global sequence, dble has set increment column values for you.");
            context.setNeedSkipError(true);
        }
        values.set(incrementIndex, new SQLIntegerExpr(val));
    }

    protected String toString(List<SQLExpr> values, boolean isFirst) {
        StringBuilder sbValues = new StringBuilder();
        if (!isFirst) {
            sbValues.append(",");
        }
        sbValues.append("(");
        for (int i = 0; i < values.size(); i++) {
            if (i != 0) {
                sbValues.append(",");
            }
            sbValues.append(values.get(i).toString());
        }
        sbValues.append(")");
        return sbValues.toString();
    }

}
