package com.actiontech.dble.manager.dump.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDataNode;
import com.actiontech.dble.manager.dump.DumpFileContext;
import com.actiontech.dble.route.factory.RouteStrategyFactory;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateDatabaseStatement;
import com.alibaba.druid.sql.ast.statement.SQLUseStatement;

import java.sql.SQLSyntaxErrorException;
import java.util.Map;

public class SchemaHandler extends DefaultHandler {

    @Override
    public SQLStatement preHandle(DumpFileContext context, String stmt) throws SQLSyntaxErrorException {
        String schema;
        stmt = stmt.replace("/*!", "/*#");
        SQLStatement sqlStatement = RouteStrategyFactory.getRouteStrategy().parserSQL(stmt);
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
        Map<String, PhysicalDataNode> dbs = DbleServer.getInstance().getConfig().getDataNodes();
        for (String dataNode : context.getAllDataNodes()) {
            context.getWriter().write(dataNode, stmt.replace("`" + schema + "`", "`" + dbs.get(dataNode).getDatabase() + "`"), false, true);
        }
    }

}
