package com.actiontech.dble.manager.dump.handler;

import com.actiontech.dble.manager.dump.DumpFileContext;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;

import java.sql.SQLNonTransientException;
import java.util.Date;
import java.util.List;

public class GlobalValuesHandler extends DefaultValuesHandler {

    private long time = new Date().getTime();

    @Override
    public void process(DumpFileContext context, List<SQLExpr> values, boolean isFirst) throws SQLNonTransientException, InterruptedException {
        // increment column
        super.process(context, values, isFirst);

        values.add(new SQLIntegerExpr(time));
    }

}
