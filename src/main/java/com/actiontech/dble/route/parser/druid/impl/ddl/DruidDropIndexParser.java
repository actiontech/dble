/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.parser.druid.impl.ddl;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.parser.druid.ServerSchemaStatVisitor;
import com.actiontech.dble.route.parser.druid.impl.DruidImplicitCommitParser;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.server.util.SchemaUtil.SchemaInfo;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropIndexStatement;

import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.Optional;

/**
 * @author huqing.yan
 */
public class DruidDropIndexParser extends DruidImplicitCommitParser {
    @Override
    public SchemaConfig doVisitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, ServerSchemaStatVisitor visitor, ShardingService service, boolean isExplain) throws SQLException {
        rrs.setOnline(true);
        String schemaName = schema == null ? null : schema.getName();
        SQLDropIndexStatement dropStmt = (SQLDropIndexStatement) stmt;
        Optional.ofNullable(dropStmt.getTableName()).orElseThrow(() ->
                new SQLSyntaxErrorException("You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near ' '", "42000", ErrorCode.ER_PARSE_ERROR));
        SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(service.getUser(), schemaName, dropStmt.getTableName());
        String statement = RouterUtil.removeSchema(rrs.getStatement(), schemaInfo.getSchema());
        rrs.setStatement(statement);
        if (RouterUtil.tryRouteToSingleDDLNode(schemaInfo, rrs, schemaInfo.getTable())) {
            return schemaInfo.getSchemaConfig();
        }
        RouterUtil.routeToDDLNode(schemaInfo, rrs);
        return schemaInfo.getSchemaConfig();
    }
}
