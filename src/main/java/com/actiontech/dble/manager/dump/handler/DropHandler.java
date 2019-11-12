package com.actiontech.dble.manager.dump.handler;

import com.actiontech.dble.manager.dump.DumpException;
import com.actiontech.dble.manager.dump.DumpFileContext;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropTableStatement;

public class DropHandler extends DefaultHandler {

    @Override
    public boolean preHandle(DumpFileContext context, SQLStatement sqlStatement) throws DumpException, InterruptedException {
        SQLDropTableStatement drop = (SQLDropTableStatement) sqlStatement;
        String tableName = drop.getTableSources().get(0).getName().getSimpleName();
        tableName = StringUtil.removeBackQuote(tableName);
        context.setTable(tableName);
        return super.preHandle(context, sqlStatement);
    }

    @Override
    public void handle(DumpFileContext context, SQLStatement sqlStatement) throws InterruptedException {
        super.handle(context, null);
    }

}
