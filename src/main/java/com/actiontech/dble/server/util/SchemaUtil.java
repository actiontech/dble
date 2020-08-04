/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.util;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.user.UserConfig;
import com.actiontech.dble.config.model.user.UserName;
import com.actiontech.dble.config.privileges.ShardingPrivileges;
import com.actiontech.dble.plan.common.item.function.ItemCreate;
import com.actiontech.dble.plan.common.ptr.StringPtr;
import com.actiontech.dble.route.parser.druid.ServerSchemaStatVisitor;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by magicdoom on 2016/1/26.
 */
public final class SchemaUtil {
    private SchemaUtil() {
    }

    public static final HashSet<String> MYSQL_SYS_SCHEMA = new HashSet<>(4, 1);

    static {
        MYSQL_SYS_SCHEMA.addAll(Arrays.asList("MYSQL", "INFORMATION_SCHEMA", "PERFORMANCE_SCHEMA", "SYS"));
    }


    public static String getRandomDb() {
        Map<String, SchemaConfig> schemaConfigMap = DbleServer.getInstance().getConfig().getSchemas();
        if (!schemaConfigMap.isEmpty()) {
            return schemaConfigMap.entrySet().iterator().next().getKey();
        }
        return null;
    }

    public static SchemaInfo getSchemaInfoWithoutCheck(String schema, String tableName) {
        SchemaInfo schemaInfo = new SchemaInfo();
        SchemaConfig config = DbleServer.getInstance().getConfig().getSchemas().get(schema);
        schemaInfo.schemaConfig = config;
        schemaInfo.table = tableName;
        schemaInfo.schema = schema;
        return schemaInfo;
    }


    public static SchemaInfo getSchemaInfo(UserName user, SchemaConfig schemaConfig, String fullTableName) throws SQLException {
        SchemaInfo schemaInfo = new SchemaInfo();
        if (DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
            fullTableName = fullTableName.toLowerCase();
        }
        String[] tableAndSchema = fullTableName.split("\\.");

        if (tableAndSchema.length == 2) {
            schemaInfo.schema = StringUtil.removeBackQuote(tableAndSchema[0]);
            schemaInfo.table = StringUtil.removeBackQuote(tableAndSchema[1]);
            UserConfig userConfig = DbleServer.getInstance().getConfig().getUsers().get(user);
            userConfig.isValidSchemaInfo(user, schemaInfo);
        } else {
            schemaInfo.schema = schemaConfig.getName();
            schemaInfo.table = StringUtil.removeBackQuote(tableAndSchema[0]);
            schemaInfo.schemaConfig = schemaConfig;
        }

        return schemaInfo;
    }

    public static SchemaInfo getSchemaInfo(UserName user, String schema, SQLExpr expr, String tableAlias) throws SQLException {
        SchemaInfo schemaInfo = new SchemaInfo();
        if (expr instanceof SQLPropertyExpr) {
            SQLPropertyExpr propertyExpr = (SQLPropertyExpr) expr;
            schemaInfo.schema = StringUtil.removeBackQuote(propertyExpr.getOwner().toString());
            schemaInfo.table = StringUtil.removeBackQuote(propertyExpr.getName());
        } else if (expr instanceof SQLIdentifierExpr) {
            SQLIdentifierExpr identifierExpr = (SQLIdentifierExpr) expr;
            schemaInfo.schema = schema;
            schemaInfo.table = StringUtil.removeBackQuote(identifierExpr.getName());
            if (identifierExpr.getName().equalsIgnoreCase("dual") && tableAlias == null) {
                schemaInfo.dual = true;
            }
        }
        schemaInfo.tableAlias = tableAlias == null ? schemaInfo.table : StringUtil.removeBackQuote(tableAlias);
        if (schemaInfo.schema == null && !schemaInfo.dual) {
            String msg = "No database selected";
            throw new SQLException(msg, "3D000", ErrorCode.ER_NO_DB_ERROR);
        }
        if (DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
            schemaInfo.table = schemaInfo.table.toLowerCase();
            schemaInfo.schema = schemaInfo.schema.toLowerCase();
        }

        UserConfig userConfig = DbleServer.getInstance().getConfig().getUsers().get(user);
        userConfig.isValidSchemaInfo(user, schemaInfo);
        return schemaInfo;
    }

    public static SchemaInfo getSchemaInfo(UserName user, String schema, SQLExprTableSource tableSource) throws SQLException {
        return getSchemaInfo(user, schema, tableSource.getExpr(), tableSource.getAlias());
    }

    public static boolean isNoSharding(ShardingService service, SQLSelectQuery sqlSelectQuery, SQLStatement selectStmt, SQLStatement childSelectStmt, String contextSchema, Set<String> schemas, StringPtr shardingNode)
            throws SQLException {
        if (sqlSelectQuery instanceof MySqlSelectQueryBlock) {
            MySqlSelectQueryBlock mySqlSelectQueryBlock = (MySqlSelectQueryBlock) sqlSelectQuery;
            //CHECK IF THE SELECT LIST HAS INNER_FUNC IN,WITCH SHOULD BE DEAL BY DBLE
            for (SQLSelectItem item : mySqlSelectQueryBlock.getSelectList()) {
                if (item.getExpr() instanceof SQLMethodInvokeExpr) {
                    if (ItemCreate.getInstance().isInnerFunc(((SQLMethodInvokeExpr) item.getExpr()).getMethodName())) {
                        return false;
                    }
                }
            }
            return isNoSharding(service, mySqlSelectQueryBlock.getFrom(), selectStmt, childSelectStmt, contextSchema, schemas, shardingNode);
        } else if (sqlSelectQuery instanceof SQLUnionQuery) {
            return isNoSharding(service, (SQLUnionQuery) sqlSelectQuery, selectStmt, contextSchema, schemas, shardingNode);
        } else {
            return false;
        }
    }

    private static boolean isNoSharding(ShardingService service, SQLUnionQuery sqlSelectQuery, SQLStatement stmt, String contextSchema, Set<String> schemas, StringPtr shardingNode)
            throws SQLException {
        SQLSelectQuery left = sqlSelectQuery.getLeft();
        SQLSelectQuery right = sqlSelectQuery.getRight();
        return isNoSharding(service, left, stmt, new SQLSelectStatement(new SQLSelect(left)), contextSchema, schemas, shardingNode) && isNoSharding(service, right, stmt, new SQLSelectStatement(new SQLSelect(right)), contextSchema, schemas, shardingNode);
    }

    public static boolean isNoSharding(ShardingService service, SQLTableSource tables, SQLStatement stmt, SQLStatement childSelectStmt, String contextSchema, Set<String> schemas, StringPtr shardingNode)
            throws SQLException {
        if (tables != null) {
            if (tables instanceof SQLExprTableSource) {
                if (!isNoSharding(service, (SQLExprTableSource) tables, stmt, childSelectStmt, contextSchema, schemas, shardingNode)) {
                    return false;
                }
            } else if (tables instanceof SQLJoinTableSource) {
                if (!isNoSharding(service, (SQLJoinTableSource) tables, stmt, childSelectStmt, contextSchema, schemas, shardingNode)) {
                    return false;
                }
            } else if (tables instanceof SQLSubqueryTableSource) {
                SQLSelectQuery sqlSelectQuery = ((SQLSubqueryTableSource) tables).getSelect().getQuery();
                if (!isNoSharding(service, sqlSelectQuery, stmt, new SQLSelectStatement(new SQLSelect(sqlSelectQuery)), contextSchema, schemas, shardingNode)) {
                    return false;
                }
            } else if (tables instanceof SQLUnionQueryTableSource) {
                if (!isNoSharding(service, ((SQLUnionQueryTableSource) tables).getUnion(), stmt, contextSchema, schemas, shardingNode)) {
                    return false;
                }
            } else {
                return false;
            }
        }
        ServerSchemaStatVisitor queryTableVisitor = new ServerSchemaStatVisitor();
        childSelectStmt.accept(queryTableVisitor);
        for (SQLSelect sqlSelect : queryTableVisitor.getSubQueryList()) {
            if (!isNoSharding(service, sqlSelect.getQuery(), stmt, new SQLSelectStatement(sqlSelect), contextSchema, schemas, shardingNode)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isNoSharding(ShardingService service, SQLExprTableSource table, SQLStatement stmt, SQLStatement childSelectStmt, String contextSchema, Set<String> schemas, StringPtr shardingNode)
            throws SQLException {
        SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(service.getUser(), contextSchema, table);
        String currentSchema = schemaInfo.schema.toUpperCase();
        if (SchemaUtil.MYSQL_SYS_SCHEMA.contains(currentSchema)) {
            schemas.add(currentSchema);
            return false;
        }

        ShardingPrivileges.CheckType checkType = ShardingPrivileges.CheckType.SELECT;
        if (childSelectStmt instanceof MySqlUpdateStatement) {
            checkType = ShardingPrivileges.CheckType.UPDATE;
        } else if (childSelectStmt instanceof SQLSelectStatement) {
            checkType = ShardingPrivileges.CheckType.SELECT;
        } else if (childSelectStmt instanceof MySqlDeleteStatement) {
            checkType = ShardingPrivileges.CheckType.DELETE;
        }

        if (!ShardingPrivileges.checkPrivilege(service.getUserConfig(), schemaInfo.schema, schemaInfo.table, checkType)) {
            String msg = "The statement DML privilege check is not passed, sql:" + stmt.toString().replaceAll("[\\t\\n\\r]", " ");
            throw new SQLNonTransientException(msg);
        }
        String noShardingNode = RouterUtil.isNoSharding(schemaInfo.schemaConfig, schemaInfo.table);
        schemas.add(schemaInfo.schema);
        if (noShardingNode == null) {
            return false;
        } else if (shardingNode.get() == null) {
            shardingNode.set(noShardingNode);
            return true;
        } else {
            return shardingNode.get().equals(noShardingNode);
        }
    }

    public static boolean isNoSharding(ShardingService service, SQLJoinTableSource tables, SQLStatement stmt, SQLStatement childSelectStmt, String contextSchema, Set<String> schemas, StringPtr shardingNode)
            throws SQLException {
        SQLTableSource left = tables.getLeft();
        SQLTableSource right = tables.getRight();
        return isNoSharding(service, left, stmt, childSelectStmt, contextSchema, schemas, shardingNode) && isNoSharding(service, right, stmt, childSelectStmt, contextSchema, schemas, shardingNode);
    }

    public static class SchemaInfo {
        public SchemaInfo() {

        }

        public SchemaInfo(String schema, String table) {
            this.schema = schema;
            this.table = table;
            this.schemaConfig = DbleServer.getInstance().getConfig().getSchemas().get(schema);
        }

        private String table;
        private String schema;
        private SchemaConfig schemaConfig;
        private boolean dual = false;
        private String tableAlias;

        @Override
        public String toString() {
            return "SchemaInfo{" + "table='" + table + '\'' +
                    ", schema='" + schema + '\'' +
                    '}';
        }

        public String getTableAlias() {
            return tableAlias;
        }

        public String getTable() {
            return table;
        }

        public String getSchema() {
            return schema;
        }

        public void setTable(String table) {
            this.table = table;
        }

        public void setSchema(String schema) {
            this.schema = schema;
        }

        public void setSchemaConfig(SchemaConfig schemaConfig) {
            this.schemaConfig = schemaConfig;
        }

        public SchemaConfig getSchemaConfig() {
            return schemaConfig;
        }

        public boolean isDual() {
            return dual;
        }
    }
}
