/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.route.parser.druid.impl.ddl;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.config.model.sharding.SchemaConfig;
import com.oceanbase.obsharding_d.route.RouteResultset;
import com.oceanbase.obsharding_d.route.parser.druid.ServerSchemaStatVisitor;
import com.oceanbase.obsharding_d.route.parser.druid.impl.DruidImplicitCommitParser;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;
import com.oceanbase.obsharding_d.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropDatabaseStatement;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;

public class DruidDropDatabaseParser extends DruidImplicitCommitParser {

    public SchemaConfig doVisitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, ServerSchemaStatVisitor visitor, ShardingService service, boolean isExplain) throws SQLException {
        SQLDropDatabaseStatement statement = (SQLDropDatabaseStatement) stmt;
        String dropSchema = StringUtil.removeBackQuote(statement.getName().getSimpleName());
        SchemaConfig sc = OBsharding_DServer.getInstance().getConfig().getSchemas().get(dropSchema);
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
