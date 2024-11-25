/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.route.parser.druid.impl.ddl;

import com.oceanbase.obsharding_d.cluster.values.DDLInfo;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.config.model.sharding.SchemaConfig;
import com.oceanbase.obsharding_d.meta.TableMeta;
import com.oceanbase.obsharding_d.route.RouteResultset;
import com.oceanbase.obsharding_d.route.parser.druid.ServerSchemaStatVisitor;
import com.oceanbase.obsharding_d.route.parser.druid.impl.DruidImplicitCommitParser;
import com.oceanbase.obsharding_d.route.util.RouterUtil;
import com.oceanbase.obsharding_d.server.util.SchemaUtil;
import com.oceanbase.obsharding_d.server.util.SchemaUtil.SchemaInfo;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;
import com.oceanbase.obsharding_d.singleton.ProxyMeta;
import com.oceanbase.obsharding_d.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLAssignItem;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;


public class DruidCreateTableParser extends DruidImplicitCommitParser {
    @Override
    public SchemaConfig doVisitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, ServerSchemaStatVisitor visitor, ShardingService service, boolean isExplain) throws SQLException {
        MySqlCreateTableStatement createStmt = (MySqlCreateTableStatement) stmt;
        rrs.setDdlType(DDLInfo.DDLType.CREATE_TABLE);
        //disable create table select from
        if (createStmt.getSelect() != null) {
            String msg = "create table from other table not supported :" + stmt;
            LOGGER.info(msg);
            throw new SQLNonTransientException(msg);
        }
        String schemaName = schema == null ? null : schema.getName();
        SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(service.getUser(), schemaName, createStmt.getTableSource());
        TableMeta tableMeta = ProxyMeta.getInstance().getTmManager().getSyncTableMeta(schemaInfo.getSchema(), schemaInfo.getTable());
        if (tableMeta != null && !createStmt.isIfNotExists()) {
            String msg = "Table '" + schemaInfo.getSchema() + "." + schemaInfo.getTable() + "' or table meta already exists";
            throw new SQLException(msg, "42S01", ErrorCode.ER_TABLE_EXISTS_ERROR);
        }

        String statement;
        if (createStmt.getLike() != null) {
            SchemaInfo likeSchemaInfo = SchemaUtil.getSchemaInfo(service.getUser(), schemaName, createStmt.getLike());
            TableMeta likeTableMeta = ProxyMeta.getInstance().getTmManager().getSyncTableMeta(likeSchemaInfo.getSchema(), likeSchemaInfo.getTable());
            if (likeTableMeta == null) {
                String msg = "Table '" + likeSchemaInfo.getSchema() + "." + likeSchemaInfo.getTable() + "' or table meta doesn't exist";
                throw new SQLException(msg, "42S02", ErrorCode.ER_NO_SUCH_TABLE);
            }
            statement = likeTableMeta.getCreateSql().replaceFirst("`" + likeSchemaInfo.getTable() + "`", "`" + schemaInfo.getTable() + "`");
        } else {
            statement = RouterUtil.removeSchema(rrs.getStatement(), schemaInfo.getSchema());
        }
        rrs.setStatement(statement);
        if (RouterUtil.tryRouteToSingleDDLNode(schemaInfo, rrs, schemaInfo.getTable())) {
            return schemaInfo.getSchemaConfig();
        }
        shardingTableCheck(createStmt);
        try {
            RouterUtil.routeToDDLNode(schemaInfo, rrs);
        } catch (SQLException e) {
            String msg = "Table '" + schemaInfo.getSchema() + "." + schemaInfo.getTable() + "' doesn't exist in the config of sharding";
            throw new SQLException(msg, "42S02", ErrorCode.ER_NO_SUCH_TABLE);
        }
        return schemaInfo.getSchemaConfig();
    }

    private void shardingTableCheckHelp(SQLAssignItem sqlAssignItem, MySqlCreateTableStatement createStmt) throws SQLNonTransientException {
        String sqlAssignItemTarget = sqlAssignItem.getTarget().toString();
        String sqlAssignItemValue = sqlAssignItem.getValue().toString();
        //ALLOW InnoDB ONLY
        if (StringUtil.equals("ENGINE", sqlAssignItemTarget) && !"InnoDB".equalsIgnoreCase(sqlAssignItemValue)) {
            String msg = "create table only can use ENGINE InnoDB, others not supported:" + createStmt;
            LOGGER.info(msg);
            throw new SQLNonTransientException(msg);
        }
        //DISABLE DATA DIRECTORY
        if (StringUtil.equals("DATA DIRECTORY", sqlAssignItemTarget)) {
            String msg = "create table with DATA DIRECTORY not supported:" + createStmt;
            LOGGER.info(msg);
            throw new SQLNonTransientException(msg);
        }


    }

    private void shardingTableCheck(MySqlCreateTableStatement createStmt) throws SQLNonTransientException {
        if (createStmt.getTableOptions().size() == 0) return;
        for (SQLAssignItem sqlAssignItem : createStmt.getTableOptions()) {
            shardingTableCheckHelp(sqlAssignItem, createStmt);
        }
    }

}
