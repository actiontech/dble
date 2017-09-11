/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.parser.druid.impl;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.ServerPrivileges;
import com.actiontech.dble.config.ServerPrivileges.Checktype;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.plan.common.ptr.StringPtr;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.parser.druid.ServerSchemaStatVisitor;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.server.util.SchemaUtil.SchemaInfo;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUnionQuery;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;

public class DruidSingleUnitSelectParser extends DefaultDruidParser {
    @Override
    public SchemaConfig visitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt,
                                     ServerSchemaStatVisitor visitor, ServerConnection sc) throws SQLException {
        SQLSelectStatement selectStmt = (SQLSelectStatement) stmt;
        SQLSelectQuery sqlSelectQuery = selectStmt.getSelect().getQuery();
        String schemaName = schema == null ? null : schema.getName();
        if (sqlSelectQuery instanceof MySqlSelectQueryBlock) {
            MySqlSelectQueryBlock mysqlSelectQuery = (MySqlSelectQueryBlock) selectStmt.getSelect().getQuery();
            SQLTableSource mysqlFrom = mysqlSelectQuery.getFrom();
            if (mysqlFrom == null) {
                RouterUtil.routeNoNameTableToSingleNode(rrs, schema);
                return schema;
            }
            if (mysqlFrom instanceof SQLSubqueryTableSource || mysqlFrom instanceof SQLJoinTableSource || mysqlFrom instanceof SQLUnionQueryTableSource) {
                StringPtr sqlSchema = new StringPtr(null);
                if (SchemaUtil.isNoSharding(sc, selectStmt.getSelect().getQuery(), selectStmt, schemaName, sqlSchema)) {
                    String realSchema = sqlSchema.get() == null ? schemaName : sqlSchema.get();
                    SchemaConfig schemaConfig = DbleServer.getInstance().getConfig().getSchemas().get(realSchema);
                    rrs.setStatement(RouterUtil.removeSchema(rrs.getStatement(), realSchema));
                    RouterUtil.routeToSingleNode(rrs, schemaConfig.getDataNode());
                    return schemaConfig;
                } else {
                    super.visitorParse(schema, rrs, stmt, visitor, sc);
                    return schema;
                }
            }

            SQLExprTableSource fromSource = (SQLExprTableSource) mysqlFrom;
            SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(sc.getUser(), schemaName, fromSource);
            if (schemaInfo.getSchemaConfig() == null) {
                String msg = "No Supported, sql:" + stmt;
                throw new SQLNonTransientException(msg);
            }
            if (!ServerPrivileges.checkPrivilege(sc, schemaInfo.getSchema(), schemaInfo.getTable(), Checktype.SELECT)) {
                String msg = "The statement DML privilege check is not passed, sql:" + stmt;
                throw new SQLNonTransientException(msg);
            }
            rrs.setStatement(RouterUtil.removeSchema(rrs.getStatement(), schemaInfo.getSchema()));
            schema = schemaInfo.getSchemaConfig();
            super.visitorParse(schema, rrs, stmt, visitor, sc);
            if (visitor.isHasSubQuery()) {
                this.getCtx().getRouteCalculateUnits().clear();
            }
            // change canRunInReadDB
            if ((mysqlSelectQuery.isForUpdate() || mysqlSelectQuery.isLockInShareMode()) && !sc.isAutocommit()) {
                rrs.setCanRunInReadDB(false);
            }
        } else if (sqlSelectQuery instanceof MySqlUnionQuery) {
            StringPtr sqlSchema = new StringPtr(null);
            if (SchemaUtil.isNoSharding(sc, selectStmt.getSelect().getQuery(), selectStmt, schemaName, sqlSchema)) {
                String realSchema = sqlSchema.get() == null ? schemaName : sqlSchema.get();
                SchemaConfig schemaConfig = DbleServer.getInstance().getConfig().getSchemas().get(realSchema);
                rrs.setStatement(RouterUtil.removeSchema(rrs.getStatement(), realSchema));
                RouterUtil.routeToSingleNode(rrs, schemaConfig.getDataNode());
                return schemaConfig;
            } else {
                super.visitorParse(schema, rrs, stmt, visitor, sc);
            }
        }
        return schema;
    }
}
