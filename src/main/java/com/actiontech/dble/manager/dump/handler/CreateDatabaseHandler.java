package com.actiontech.dble.manager.dump.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDBNode;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.manager.dump.DumpException;
import com.actiontech.dble.manager.dump.DumpFileContext;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateDatabaseStatement;

import java.util.Map;
import java.util.Set;

public class CreateDatabaseHandler implements StatementHandler {

    @Override
    public void handle(DumpFileContext context, SQLStatement sqlStatement) throws DumpException, InterruptedException {
        SQLCreateDatabaseStatement createDatabase = (SQLCreateDatabaseStatement) sqlStatement;
        String schema = StringUtil.removeBackQuote(createDatabase.getName().getSimpleName());
        SchemaConfig schemaConfig = DbleServer.getInstance().getConfig().getSchemas().get(schema);
        if (schemaConfig == null) {
            throw new DumpException("schema[" + schema + "] doesn't exist in config.");
        }
        context.setSchema(schema);
        context.setDefaultDataNode(schemaConfig.getDataNode());
        context.setTable(null);

        Set<String> allDataNodes = DbleServer.getInstance().getConfig().getSchemas().get(schema).getAllDataNodes();
        Map<String, PhysicalDBNode> dbs = DbleServer.getInstance().getConfig().getDataNodes();
        for (String dataNode : allDataNodes) {
            context.getWriter().write(dataNode, context.getStmt().replace("`" + schema + "`", "`" + dbs.get(dataNode).getDatabase() + "`"));
        }
    }

}
