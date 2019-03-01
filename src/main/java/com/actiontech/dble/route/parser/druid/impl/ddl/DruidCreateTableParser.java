/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.parser.druid.impl.ddl;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.DDLInfo;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.meta.protocol.StructureMeta;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.parser.druid.ServerSchemaStatVisitor;
import com.actiontech.dble.route.parser.druid.impl.DefaultDruidParser;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.util.GlobalTableUtil;
import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.server.util.SchemaUtil.SchemaInfo;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;


public class DruidCreateTableParser extends DefaultDruidParser {
    @Override
    public SchemaConfig visitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, ServerSchemaStatVisitor visitor, ServerConnection sc)
            throws SQLException {
        MySqlCreateTableStatement createStmt = (MySqlCreateTableStatement) stmt;
        rrs.setDdlType(DDLInfo.DDLType.CREATE_TABLE);
        //disable create table select from
        if (createStmt.getSelect() != null) {
            String msg = "create table from other table not supported :" + stmt;
            LOGGER.info(msg);
            throw new SQLNonTransientException(msg);
        }

        String schemaName = schema == null ? null : schema.getName();
        SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(sc.getUser(), schemaName, createStmt.getTableSource());
        StructureMeta.TableMeta tableMeta = DbleServer.getInstance().getTmManager().getSyncTableMeta(schemaInfo.getSchema(), schemaInfo.getTable());
        if (tableMeta != null && !createStmt.isIfNotExiists()) {
            String msg = "Table '" + schemaInfo.getSchema() + "." + schemaInfo.getTable() + "' or table meta already exists";
            throw new SQLException(msg, "42S01", ErrorCode.ER_TABLE_EXISTS_ERROR);
        }

        String statement;
        if (createStmt.getLike() != null) {
            SchemaInfo likeSchemaInfo = SchemaUtil.getSchemaInfo(sc.getUser(), schemaName, createStmt.getLike());
            StructureMeta.TableMeta likeTableMeta = DbleServer.getInstance().getTmManager().getSyncTableMeta(likeSchemaInfo.getSchema(), likeSchemaInfo.getTable());
            if (likeTableMeta == null) {
                String msg = "Table '" + likeSchemaInfo.getSchema() + "." + likeSchemaInfo.getTable() + "' or table meta doesn't exist";
                throw new SQLException(msg, "42S02", ErrorCode.ER_NO_SUCH_TABLE);
            }
            statement = likeTableMeta.getCreateSql().replaceFirst("`" + likeSchemaInfo.getTable() + "`", "`" + schemaInfo.getTable() + "`");
        } else {
            statement = RouterUtil.removeSchema(rrs.getStatement(), schemaInfo.getSchema());
        }

        rrs.setStatement(statement);
        String noShardingNode = RouterUtil.isNoShardingDDL(schemaInfo.getSchemaConfig(), schemaInfo.getTable());
        if (noShardingNode != null) {
            RouterUtil.routeToSingleDDLNode(schemaInfo, rrs, noShardingNode);
            return schemaInfo.getSchemaConfig();
        }
        sharingTableCheck(createStmt);
        if (GlobalTableUtil.useGlobalTableCheck() &&
                GlobalTableUtil.isGlobalTable(schemaInfo.getSchemaConfig(), schemaInfo.getTable())) {
            String sql = addColumnIfCreate(createStmt);
            rrs.setSrcStatement(sql);
            sql = RouterUtil.removeSchema(sql, schemaInfo.getSchema());
            rrs.setStatement(sql);
        }
        try {
            RouterUtil.routeToDDLNode(schemaInfo, rrs);
        } catch (SQLException e) {
            String msg = "Table '" + schemaInfo.getSchema() + "." + schemaInfo.getTable() + "' doesn't exist in the config of schema";
            throw new SQLException(msg, "42S02", ErrorCode.ER_NO_SUCH_TABLE);
        }
        return schemaInfo.getSchemaConfig();
    }

    private String addColumnIfCreate(MySqlCreateTableStatement createStmt) {
        removeGlobalColumnIfExist(createStmt);
        createStmt.getTableElementList().add(GlobalTableUtil.createCheckColumn());
        return createStmt.toString();
    }

    private static void removeGlobalColumnIfExist(SQLCreateTableStatement statement) {
        for (SQLTableElement tableElement : statement.getTableElementList()) {
            SQLName sqlName = null;
            if (tableElement instanceof SQLColumnDefinition) {
                sqlName = ((SQLColumnDefinition) tableElement).getName();
            }
            if (sqlName != null) {
                String simpleName = sqlName.getSimpleName();
                simpleName = StringUtil.removeBackQuote(simpleName);
                if (GlobalTableUtil.GLOBAL_TABLE_CHECK_COLUMN.equalsIgnoreCase(simpleName)) {
                    statement.getTableElementList().remove(tableElement);
                    break;
                }
            }
        }
    }

    /**
     */
    private void sharingTableCheck(MySqlCreateTableStatement createStmt) throws SQLNonTransientException {
        //ALLOW InnoDB ONLY
        SQLObject engine = createStmt.getTableOptions().get("ENGINE");
        if (engine != null) {
            String strEngine;
            if (engine instanceof SQLCharExpr) {
                strEngine = ((SQLCharExpr) engine).getText();
            } else if (engine instanceof SQLIdentifierExpr) {
                strEngine = ((SQLIdentifierExpr) engine).getSimpleName();
            } else {
                strEngine = engine.toString();
            }
            if (!"InnoDB".equalsIgnoreCase(strEngine)) {
                String msg = "create table only can use ENGINE InnoDB,others not supported:" + createStmt;
                LOGGER.info(msg);
                throw new SQLNonTransientException(msg);
            }
        }

        //DISABLE DATA DIRECTORY
        if (createStmt.getTableOptions().get("DATA DIRECTORY") != null) {
            String msg = "create table with DATA DIRECTORY  not supported:" + createStmt;
            LOGGER.info(msg);
            throw new SQLNonTransientException(msg);
        }
    }
}
