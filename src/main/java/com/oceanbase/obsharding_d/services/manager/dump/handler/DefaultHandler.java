/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.dump.handler;

import com.oceanbase.obsharding_d.services.manager.dump.DumpFileContext;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;

import java.sql.SQLNonTransientException;

public class DefaultHandler implements StatementHandler {

    @Override
    public SQLStatement preHandle(DumpFileContext context, String stmt) throws SQLNonTransientException {
        return null;
    }

    @Override
    public void handle(DumpFileContext context, SQLStatement sqlStatement) throws InterruptedException {
        for (String shardingNode : context.getTableConfig().getShardingNodes()) {
            context.getWriter().write(shardingNode, SQLUtils.toMySqlString(sqlStatement));
        }
    }

    @Override
    public void handle(DumpFileContext context, String stmt) throws InterruptedException {
        if (context.getTable() == null) {
            context.getWriter().writeAll(stmt);
        } else if (context.getTableConfig() == null) {
            context.getWriter().write(context.getDefaultShardingNodes().get(0), stmt);
        } else {
            for (String shardingNode : context.getTableConfig().getShardingNodes()) {
                context.getWriter().write(shardingNode, stmt);
            }
        }
    }
}
