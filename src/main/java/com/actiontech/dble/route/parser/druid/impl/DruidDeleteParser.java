/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.parser.druid.impl;

import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.sharding.table.BaseTableConfig;
import com.actiontech.dble.config.model.sharding.table.GlobalTableConfig;
import com.actiontech.dble.config.privileges.ShardingPrivileges;
import com.actiontech.dble.config.privileges.ShardingPrivileges.CheckType;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.parser.druid.ServerSchemaStatVisitor;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.server.util.SchemaUtil.SchemaInfo;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.Collection;
import java.util.List;

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
            for (SchemaInfo schemaInfo : schemaInfos) {
                BaseTableConfig tc = schemaInfo.getSchemaConfig().getTables().get(schemaInfo.getTable());
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

            RouterUtil.routeToMultiNode(false, rrs, routeShardingNodes, true);
            rrs.setFinishedRoute(true);
            return schema;

        } else {
            SQLExprTableSource deleteTableSource = (SQLExprTableSource) tableSource;
            SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(service.getUser(), schemaName, deleteTableSource);
            if (!ShardingPrivileges.checkPrivilege(service.getUserConfig(), schemaInfo.getSchema(), schemaInfo.getTable(), CheckType.DELETE)) {
                String msg = "The statement DML privilege check is not passed, sql:" + stmt.toString().replaceAll("[\\t\\n\\r]", " ");
                throw new SQLNonTransientException(msg);
            }
            schema = schemaInfo.getSchemaConfig();
            BaseTableConfig tc = schema.getTables().get(schemaInfo.getTable());
            rrs.setStatement(RouterUtil.removeSchema(rrs.getStatement(), schemaInfo.getSchema()));
            super.visitorParse(schema, rrs, stmt, visitor, service, isExplain);

            if (visitor.getSubQueryList().size() > 0) {
                routeForModifySubQueryList(rrs, tc, visitor, schema, service);
                return schema;
            }
            String tableName = schemaInfo.getTable();
            String noShardingNode = RouterUtil.isNoSharding(schema, tableName);
            if (noShardingNode != null) {
                RouterUtil.routeToSingleNode(rrs, noShardingNode);
                return schema;
            }
            checkTableExists(tc, schema.getName(), tableName, CheckType.DELETE);

            if (tc instanceof GlobalTableConfig) {
                RouterUtil.routeToMultiNode(false, rrs, tc.getShardingNodes(), true);
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
