package com.actiontech.dble.manager.dump.handler;

import com.actiontech.dble.manager.dump.DumpException;
import com.actiontech.dble.manager.dump.DumpFileContext;
import com.alibaba.druid.sql.ast.SQLStatement;

public interface StatementHandler {

    boolean preHandle(DumpFileContext context, SQLStatement statement) throws DumpException, InterruptedException;

    void handle(DumpFileContext context, SQLStatement statement) throws DumpException, InterruptedException;

}
