/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.parser.druid.impl;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.ServerPrivileges;
import com.actiontech.dble.config.ServerPrivileges.Checktype;
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
            if (!SchemaUtil.isNoSharding(sc, (SQLJoinTableSource) tableSource, stmt, schemaName, sqlSchema)) {
                String msg = "DELETE query with multiple tables is not supported, sql:" + stmt;
                throw new SQLNonTransientException(msg);
            } else {
                if (delete.getWhere() != null && !SchemaUtil.isNoSharding(sc, delete.getWhere(), schemaName, sqlSchema)) {
                    String msg = "DELETE query with sub-query is not supported, sql:" + stmt;
                    throw new SQLNonTransientException(msg);
                }
                String realSchema = sqlSchema.get() == null ? schemaName : sqlSchema.get();
                SchemaConfig schemaConfig = DbleServer.getInstance().getConfig().getSchemas().get(realSchema);
                rrs.setStatement(RouterUtil.removeSchema(rrs.getStatement(), realSchema));
                RouterUtil.routeToSingleNode(rrs, schemaConfig.getDataNode());
                rrs.setFinishedRoute(true);
                return schema;
            }
        } else {
            SQLExprTableSource deleteTableSource = (SQLExprTableSource) tableSource;
            SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(sc.getUser(), schemaName, deleteTableSource);
            if (!ServerPrivileges.checkPrivilege(sc, schemaInfo.getSchema(), schemaInfo.getTable(), Checktype.DELETE)) {
                String msg = "The statement DML privilege check is not passed, sql:" + stmt;
                throw new SQLNonTransientException(msg);
            }
            schema = schemaInfo.getSchemaConfig();
            rrs.setStatement(RouterUtil.removeSchema(rrs.getStatement(), schemaInfo.getSchema()));
            if (RouterUtil.isNoSharding(schema, schemaInfo.getTable())) {
                if (delete.getWhere() != null && !SchemaUtil.isNoSharding(sc, delete.getWhere(), schemaName, new StringPtr(schemaInfo.getSchema()))) {
                    String msg = "DELETE query with sub-query is not supported, sql:" + stmt;
                    throw new SQLNonTransientException(msg);
                }
                RouterUtil.routeToSingleNode(rrs, schema.getDataNode());
                return schema;
            }
            super.visitorParse(schema, rrs, stmt, visitor, sc);
            if (visitor.isHasSubQuery()) {
                String msg = "DELETE query with sub-query  is not supported, sql:" + stmt;
                throw new SQLNonTransientException(msg);
            }
            TableConfig tc = schema.getTables().get(schemaInfo.getTable());
            if (tc != null && tc.isGlobalTable()) {
                RouterUtil.routeToMultiNode(false, rrs, tc.getDataNodes(), tc.isGlobalTable());
                rrs.setFinishedRoute(true);
                return schema;
            }
        }
        return schema;
    }
}
