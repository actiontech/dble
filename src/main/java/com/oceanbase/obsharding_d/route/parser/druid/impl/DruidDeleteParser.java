/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.route.parser.druid.impl;

import com.oceanbase.obsharding_d.config.model.sharding.SchemaConfig;
import com.oceanbase.obsharding_d.config.model.sharding.table.BaseTableConfig;
import com.oceanbase.obsharding_d.config.model.sharding.table.GlobalTableConfig;
import com.oceanbase.obsharding_d.config.privileges.ShardingPrivileges;
import com.oceanbase.obsharding_d.config.privileges.ShardingPrivileges.CheckType;
import com.oceanbase.obsharding_d.route.RouteResultset;
import com.oceanbase.obsharding_d.route.parser.druid.ServerSchemaStatVisitor;
import com.oceanbase.obsharding_d.route.util.RouterUtil;
import com.oceanbase.obsharding_d.server.util.SchemaUtil;
import com.oceanbase.obsharding_d.server.util.SchemaUtil.SchemaInfo;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.google.common.collect.Sets;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * see http://dev.mysql.com/doc/refman/5.7/en/delete.html
 *
 * @author huqing.yan
 */
public class DruidDeleteParser extends DruidModifyParser {

    protected static final String MODIFY_SQL_NOT_SUPPORT_MESSAGE = "This `Complex Delete Syntax` is not supported!";

    @Override
    public SchemaConfig visitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, ServerSchemaStatVisitor visitor, ShardingService service, boolean isExplain)
            throws SQLException {
        String schemaName = schema == null ? null : schema.getName();
        MySqlDeleteStatement delete = (MySqlDeleteStatement) stmt;
        SQLTableSource tableSource = delete.getTableSource();
        SQLTableSource fromSource = delete.getFrom();
        if (fromSource != null) {
            tableSource = fromSource;
        }
        if (tableSource instanceof SQLJoinTableSource) {
            super.visitorParse(schema, rrs, stmt, visitor, service, isExplain);
            if (visitor.getSubQueryList().size() > 0) {
                throw new SQLNonTransientException(MODIFY_SQL_NOT_SUPPORT_MESSAGE);
            }

            List<SchemaInfo> schemaInfos = checkPrivilegeForModifyTable(service, schemaName, stmt, visitor.getMotifyTableSourceList());

            boolean isAllGlobal = true;
            Set<String> tableSet = Sets.newHashSet();
            for (SchemaInfo schemaInfo : schemaInfos) {
                BaseTableConfig tc = schemaInfo.getSchemaConfig().getTables().get(schemaInfo.getTable());
                tableSet.add(schemaInfo.getSchema() + "." + schemaInfo.getTable());
                rrs.setStatement(RouterUtil.removeSchema(rrs.getStatement(), schemaInfo.getSchema()));
                if (tc == null || !(tc instanceof GlobalTableConfig)) {
                    isAllGlobal = false;
                }
            }

            Collection<String> routeShardingNodes;
            if (isAllGlobal) {
                routeShardingNodes = checkForMultiNodeGlobal(schemaInfos);
            } else {
                //try to route to single Node for each table
                routeShardingNodes = checkForSingleNodeTable(rrs, service.getCharset().getClient());
            }
            if (routeShardingNodes == null) {
                throw new SQLNonTransientException(getErrorMsg());
            }

            if (ctx.getTables().isEmpty()) {
                RouterUtil.routeToMultiNode(false, rrs, routeShardingNodes, true, tableSet);
            } else {
                RouterUtil.routeToMultiNode(false, rrs, routeShardingNodes, true, ctx.getTables());
            }
            rrs.setFinishedRoute(true);
            return schema;

        } else {
            SQLExprTableSource deleteTableSource = (SQLExprTableSource) tableSource;
            SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(service.getUser(), schemaName, deleteTableSource);
            if (!ShardingPrivileges.checkPrivilege(service.getUserConfig(), schemaInfo.getSchema(), schemaInfo.getTable(), CheckType.DELETE)) {
                String msg = "The statement DML privilege check is not passed, sql:" + stmt.toString().replaceAll("[\\t\\n\\r]", " ");
                throw new SQLNonTransientException(msg);
            }
            SchemaConfig originSchema = schema;
            schema = schemaInfo.getSchemaConfig();
            BaseTableConfig tc = schema.getTables().get(schemaInfo.getTable());
            rrs.setStatement(RouterUtil.removeSchema(rrs.getStatement(), schemaInfo.getSchema()));
            super.visitorParse(originSchema, rrs, stmt, visitor, service, isExplain);

            if (visitor.getSubQueryList().size() > 0) {
                routeForModifySubQueryList(rrs, tc, visitor, schema, service, originSchema);
                return schema;
            }
            String tableName = schemaInfo.getTable();
            String noShardingNode = RouterUtil.isNoSharding(schema, tableName);
            if (noShardingNode != null) {
                RouterUtil.routeToSingleNode(rrs, noShardingNode, Sets.newHashSet(schemaName + "." + tableName));
                return schema;
            }
            checkTableExists(tc, schema.getName(), tableName, CheckType.DELETE);

            if (tc instanceof GlobalTableConfig) {
                if (ctx.getTables().isEmpty()) {
                    RouterUtil.routeToMultiNode(false, rrs, tc.getShardingNodes(), true, Sets.newHashSet(schemaName + "." + tableName));
                } else {
                    RouterUtil.routeToMultiNode(false, rrs, tc.getShardingNodes(), true, ctx.getTables());
                }
                rrs.setFinishedRoute(true);
                return schema;
            }

            if (delete.getLimit() != null) {
                this.updateAndDeleteLimitRoute(rrs, tableName, schema, service.getCharset().getClient());
            }
        }
        return schema;
    }


    @Override
    SQLSelect acceptVisitor(SQLObject stmt, ServerSchemaStatVisitor visitor) {
        stmt.accept(visitor);
        return (SQLSelect) stmt;
    }

    @Override
    int tryGetShardingColIndex(SchemaInfo schemaInfo, SQLStatement stmt, String partitionColumn) throws SQLNonTransientException {
        return 0;
    }

    @Override
    String getErrorMsg() {
        return MODIFY_SQL_NOT_SUPPORT_MESSAGE;
    }
}
