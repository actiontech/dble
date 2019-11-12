package com.actiontech.dble.manager.dump.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDBNode;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.manager.dump.DumpException;
import com.actiontech.dble.manager.dump.DumpFileContext;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateDatabaseStatement;
import com.alibaba.druid.sql.ast.statement.SQLUseStatement;

import java.util.Map;
import java.util.Set;

public class SchemaHandler implements StatementHandler {

    @Override
    public boolean preHandle(DumpFileContext context, SQLStatement sqlStatement) {
        String schema;
        if (sqlStatement instanceof SQLUseStatement) {
            SQLUseStatement use = (SQLUseStatement) sqlStatement;
            schema = use.getDatabase().getSimpleName();
        } else {
            SQLCreateDatabaseStatement createDatabase = (SQLCreateDatabaseStatement) sqlStatement;
            schema = createDatabase.getName().getSimpleName();
        }
        String realSchema = StringUtil.removeBackQuote(schema);
        SchemaConfig schemaConfig = DbleServer.getInstance().getConfig().getSchemas().get(realSchema);
        if (schemaConfig == null) {
            throw new DumpException("schema[" + realSchema + "] doesn't exist in config.");
        }
        context.setSchema(realSchema);
        context.setDefaultDataNode(schemaConfig.getDataNode());
        context.setTable(null);
        return false;
    }

    @Override
    public void handle(DumpFileContext context, SQLStatement sqlStatement) throws DumpException, InterruptedException {
        String schema = context.getSchema();
        Set<String> allDataNodes = DbleServer.getInstance().getConfig().getSchemas().get(schema).getAllDataNodes();
        Map<String, PhysicalDBNode> dbs = DbleServer.getInstance().getConfig().getDataNodes();
        for (String dataNode : allDataNodes) {
            context.getWriter().write(dataNode, context.getStmt().replace("`" + schema + "`", "`" + dbs.get(dataNode).getDatabase() + "`"), false, true);
        }
    }

}
