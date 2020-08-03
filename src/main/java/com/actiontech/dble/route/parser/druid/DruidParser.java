/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.parser.druid;

import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.server.ServerConnection;
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
     * @param sc
     */
    SchemaConfig parser(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, ServerSchemaStatVisitor schemaStatVisitor, ServerConnection sc) throws SQLException;


    /**
     * use MycatSchemaStatVisitor, get the info of tables,tableAliasMap,conditions and so on
     *
     * @param schema
     * @param stmt
     * @param sc
     */
    SchemaConfig parser(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, ServerSchemaStatVisitor schemaStatVisitor, ServerConnection sc, boolean isExplain) throws SQLException;

    /**
     * @param stmt
     * @param sc
     */
    SchemaConfig visitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, ServerSchemaStatVisitor visitor, ServerConnection sc, boolean isExplain) throws SQLException;

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
