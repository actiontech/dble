/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.dump.handler;

import com.actiontech.dble.services.manager.dump.DumpException;
import com.actiontech.dble.services.manager.dump.DumpFileContext;
import com.alibaba.druid.sql.ast.SQLStatement;

import java.sql.SQLNonTransientException;

public interface StatementHandler {

    SQLStatement preHandle(DumpFileContext context, String stmt) throws DumpException, InterruptedException, SQLNonTransientException;

    void handle(DumpFileContext context, SQLStatement statement) throws DumpException, InterruptedException;

    void handle(DumpFileContext context, String stmt) throws DumpException, InterruptedException;
}
