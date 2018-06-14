/*
 * Copyright (C) 2016-2018 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.parser.druid.impl;

import com.actiontech.dble.config.ServerPrivileges;
import com.actiontech.dble.config.ServerPrivileges.CheckType;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.plan.common.ptr.StringPtr;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.parser.druid.ServerSchemaStatVisitor;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.server.util.SchemaUtil.SchemaInfo;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;

/**
 * see http://dev.mysql.com/doc/refman/5.7/en/delete.html
 *
 * @author huqing.yan
 */
public class DruidDeleteParser extends DefaultDruidParser {
    @Override
    public SchemaConfig visitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, ServerSchemaStatVisitor visitor, ServerConnection sc)
            throws SQLException {
        String schemaName = schema == null ? null : schema.getName();
        MySqlDeleteStatement delete = (MySqlDeleteStatement) stmt;
        SQLTableSource tableSource = delete.getTableSource();
        SQLTableSource fromSource = delete.getFrom();
        if (fromSource != null) {
            tableSource = fromSource;
        }
        if (tableSource instanceof SQLJoinTableSource) {
            StringPtr sqlSchema = new StringPtr(null);
            if (!SchemaUtil.isNoSharding(sc, (SQLJoinTableSource) tableSource, stmt, stmt, schemaName, sqlSchema)) {
                String msg = "DELETE query with multiple tables is not supported, sql:" + stmt;
                throw new SQLNonTransientException(msg);
            } else {
                return routeToNoSharding(schema, rrs, schemaName, sqlSchema);
            }
        } else {
            SQLExprTableSource deleteTableSource = (SQLExprTableSource) tableSource;
            SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(sc.getUser(), schemaName, deleteTableSource);
            if (!ServerPrivileges.checkPrivilege(sc, schemaInfo.getSchema(), schemaInfo.getTable(), CheckType.DELETE)) {
                String msg = "The statement DML privilege check is not passed, sql:" + stmt;
                throw new SQLNonTransientException(msg);
            }
            schema = schemaInfo.getSchemaConfig();
            rrs.setStatement(RouterUtil.removeSchema(rrs.getStatement(), schemaInfo.getSchema()));
            super.visitorParse(schema, rrs, stmt, visitor, sc);
            if (visitor.getSubQueryList().size() > 0) {
                StringPtr sqlSchema = new StringPtr(null);
                if (!SchemaUtil.isNoSharding(sc, deleteTableSource, stmt, stmt, schemaInfo.getSchema(), sqlSchema)) {
                    String msg = "DELETE query with sub-query  is not supported, sql:" + stmt;
                    throw new SQLNonTransientException(msg);
                } else {
                    return routeToNoSharding(schema, rrs, schemaName, sqlSchema);
                }
            }
            String tableName = schemaInfo.getTable();
            if (RouterUtil.isNoSharding(schema, tableName)) {
                RouterUtil.routeToSingleNode(rrs, schema.getDataNode());
                return schema;
            }
            TableConfig tc = schema.getTables().get(tableName);
            checkTableExists(tc, schema.getName(), tableName, CheckType.DELETE);

            if (tc.isGlobalTable()) {
                RouterUtil.routeToMultiNode(false, rrs, tc.getDataNodes(), tc.isGlobalTable());
                rrs.setFinishedRoute(true);
                return schema;
            }
        }
        return schema;
    }



}
