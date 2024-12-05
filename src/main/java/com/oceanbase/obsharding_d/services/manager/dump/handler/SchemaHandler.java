/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.dump.handler;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.datasource.ShardingNode;
import com.oceanbase.obsharding_d.route.parser.util.DruidUtil;
import com.oceanbase.obsharding_d.services.manager.dump.DumpFileContext;
import com.oceanbase.obsharding_d.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateDatabaseStatement;
import com.alibaba.druid.sql.ast.statement.SQLUseStatement;

import java.sql.SQLSyntaxErrorException;
import java.util.Map;

public class SchemaHandler extends DefaultHandler {

    @Override
    public SQLStatement preHandle(DumpFileContext context, String stmt) throws SQLSyntaxErrorException {
        String schema;
        stmt = stmt.replace("/*!", "/*#").replace("\r", "");
        SQLStatement sqlStatement = DruidUtil.parseMultiSQL(stmt);
        if (sqlStatement instanceof SQLUseStatement) {
            SQLUseStatement use = (SQLUseStatement) sqlStatement;
            schema = use.getDatabase().getSimpleName();
        } else {
            SQLCreateDatabaseStatement createDatabase = (SQLCreateDatabaseStatement) sqlStatement;
            schema = createDatabase.getName().getSimpleName();
        }

        schema = StringUtil.removeBackQuote(schema);
        context.setSchema(schema);
        return null;
    }

    @Override
    public void handle(DumpFileContext context, String stmt) throws InterruptedException {
        String schema = context.getSchema();
        Map<String, ShardingNode> dbs = OBsharding_DServer.getInstance().getConfig().getShardingNodes();
        for (String shardingNode : context.getAllShardingNodes()) {
            context.getWriter().write(shardingNode, stmt.replace("`" + schema + "`", "`" + dbs.get(shardingNode).getDatabase() + "`"));
        }
    }

}
