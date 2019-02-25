/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.parser.druid.impl;

import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.plan.common.ptr.StringPtr;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.parser.druid.ServerSchemaStatVisitor;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.util.SchemaUtil;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLUnionQuery;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DruidSingleUnitSelectParser extends DefaultDruidParser {

    private Map<String, SchemaConfig> schemaMap = null;

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
                StringPtr noShardingNode = new StringPtr(null);
                Set<String> schemas = new HashSet<>();
                if ((schemaMap != null && schemaMap.size() == 1) &&
                        SchemaUtil.isNoSharding(sc, selectStmt.getSelect().getQuery(), selectStmt, selectStmt, schemaName, schemas, noShardingNode)) {
                    return routeToNoSharding(schema, rrs, schemas, noShardingNode);
                } else {
                    super.visitorParse(schema, rrs, stmt, visitor, sc);
                    return schema;
                }
            }

            for (SchemaConfig schemaInfo : schemaMap.values()) {
                rrs.setStatement(RouterUtil.removeSchema(rrs.getStatement(), schemaInfo.getName()));
            }

            super.visitorParse(null, rrs, stmt, visitor, sc);
            if (visitor.getSubQueryList().size() > 0) {
                this.getCtx().getRouteCalculateUnits().clear();
            }
            // change canRunInReadDB
            if ((mysqlSelectQuery.isForUpdate() || mysqlSelectQuery.isLockInShareMode()) && !sc.isAutocommit()) {
                rrs.setCanRunInReadDB(false);
            }
        } else if (sqlSelectQuery instanceof SQLUnionQuery) {
            StringPtr noShardingNode = new StringPtr(null);
            Set<String> schemas = new HashSet<>();
            if ((schemaMap != null && schemaMap.size() == 1) &&
                    SchemaUtil.isNoSharding(sc, selectStmt.getSelect().getQuery(), selectStmt, selectStmt, schemaName, schemas, noShardingNode)) {
                return routeToNoSharding(schema, rrs, schemas, noShardingNode);
            } else {
                super.visitorParse(schema, rrs, stmt, visitor, sc);
            }
        }
        return schema;
    }


    public Map<String, SchemaConfig> getSchemaMap() {
        return schemaMap;
    }

    public void setSchemaMap(Map<String, SchemaConfig> schemaMap) {
        this.schemaMap = schemaMap;
    }
}
