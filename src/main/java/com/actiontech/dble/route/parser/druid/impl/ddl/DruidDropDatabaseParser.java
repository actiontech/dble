package com.actiontech.dble.route.parser.druid.impl.ddl;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.parser.druid.ServerSchemaStatVisitor;
import com.actiontech.dble.route.parser.druid.impl.DruidImplicitCommitParser;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropDatabaseStatement;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;

public class DruidDropDatabaseParser extends DruidImplicitCommitParser {

    public SchemaConfig doVisitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, ServerSchemaStatVisitor visitor, ShardingService service, boolean isExplain) throws SQLException {
        SQLDropDatabaseStatement statement = (SQLDropDatabaseStatement) stmt;
        String dropSchema = StringUtil.removeBackQuote(statement.getName().getSimpleName());
        SchemaConfig sc = DbleServer.getInstance().getConfig().getSchemas().get(dropSchema);
        if (sc != null) {
            if (!sc.isLogicalCreateADrop()) {
                String msg = "THE DDL is not supported :" + statement;
                throw new SQLNonTransientException(msg);
            }
            rrs.setFinishedExecute(true);
        } else {
            throw new SQLException("Can't drop database '" + dropSchema + "' that doesn't exists in config");
        }
        return schema;
    }

}
