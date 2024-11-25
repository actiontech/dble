/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.route.parser.druid;

import com.oceanbase.obsharding_d.config.model.sharding.SchemaConfig;
import com.oceanbase.obsharding_d.route.RouteResultset;

import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;
import com.alibaba.druid.sql.ast.SQLStatement;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;

/**
 * Parser SQLStatement
 *
 * @author wang.dw
 */
public interface DruidParser {
    /**
     * use MycatSchemaStatVisitor, get the info of tables,tableAliasMap,conditions and so on
     *
     * @param schema
     * @param stmt
     */
    SchemaConfig parser(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, ServerSchemaStatVisitor schemaStatVisitor, ShardingService service) throws SQLException;


    /**
     * use MycatSchemaStatVisitor, get the info of tables,tableAliasMap,conditions and so on
     *
     * @param schema
     * @param stmt
     */
    SchemaConfig parser(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, ServerSchemaStatVisitor schemaStatVisitor, ShardingService service, boolean isExplain) throws SQLException;

    /**
     * @param stmt
     */
    SchemaConfig visitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, ServerSchemaStatVisitor visitor, ShardingService service, boolean isExplain) throws SQLException;

    /**
     * changeSql: add limit
     *
     * @param schema
     * @param rrs
     * @param stmt
     * @throws SQLNonTransientException
     */
    void changeSql(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt) throws SQLException;

    /**
     * get parser info
     *
     * @return
     */
    DruidShardingParseInfo getCtx();

}
