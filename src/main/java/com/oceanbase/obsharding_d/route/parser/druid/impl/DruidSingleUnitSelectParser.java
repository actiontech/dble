/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.route.parser.druid.impl;

import com.oceanbase.obsharding_d.config.model.sharding.SchemaConfig;
import com.oceanbase.obsharding_d.plan.common.ptr.StringPtr;
import com.oceanbase.obsharding_d.route.RouteResultset;
import com.oceanbase.obsharding_d.route.parser.druid.ServerSchemaStatVisitor;
import com.oceanbase.obsharding_d.route.parser.util.Pair;
import com.oceanbase.obsharding_d.route.util.RouterUtil;

import com.oceanbase.obsharding_d.server.util.SchemaUtil;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DruidSingleUnitSelectParser extends DefaultDruidParser {

    private Map<Pair<String, String>, SchemaConfig> schemaMap = null;

    @Override
    public SchemaConfig visitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt,
                                     ServerSchemaStatVisitor visitor, ShardingService service, boolean isExplain) throws SQLException {
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
                        SchemaUtil.isNoSharding(service, selectStmt.getSelect().getQuery(), selectStmt, selectStmt, schemaName, schemas, noShardingNode)) {
                    return routeToNoSharding(schema, rrs, schemas, noShardingNode, null);
                } else {
                    super.visitorParse(schema, rrs, stmt, visitor, service, isExplain);
                    return schema;
                }
            }

            if (schemaMap != null) {
                for (SchemaConfig schemaInfo : schemaMap.values()) {
                    rrs.setStatement(RouterUtil.removeSchema(rrs.getStatement(), schemaInfo.getName()));
                }
            }

            super.visitorParse(schema, rrs, stmt, visitor, service, isExplain);
            if (visitor.getSubQueryList().size() > 0) {
                this.getCtx().clearRouteCalculateUnit();
            }
            // change canRunInReadDB
            if ((mysqlSelectQuery.isForUpdate() || mysqlSelectQuery.isLockInShareMode() || mysqlSelectQuery.isForShare())) {
                if (!service.isAutocommit()) {
                    rrs.setCanRunInReadDB(false);
                } else {
                    rrs.setForUpdate(true);
                }
            }
        } else if (sqlSelectQuery instanceof SQLUnionQuery) {
            StringPtr noShardingNode = new StringPtr(null);
            Set<String> schemas = new HashSet<>();
            if ((schemaMap != null && schemaMap.size() == 1) &&
                    SchemaUtil.isNoSharding(service, selectStmt.getSelect().getQuery(), selectStmt, selectStmt, schemaName, schemas, noShardingNode)) {
                return routeToNoSharding(schema, rrs, schemas, noShardingNode, null);
            } else {
                super.visitorParse(schema, rrs, stmt, visitor, service, isExplain);
            }
        }
        return schema;
    }


    public Map<Pair<String, String>, SchemaConfig> getSchemaMap() {
        return schemaMap;
    }

    public void setSchemaMap(Map<Pair<String, String>, SchemaConfig> schemaMap) {
        this.schemaMap = schemaMap;
    }
}
