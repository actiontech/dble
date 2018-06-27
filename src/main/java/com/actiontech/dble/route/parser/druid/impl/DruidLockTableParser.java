/*
 * Copyright (C) 2016-2018 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.parser.druid.impl;

import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.route.parser.druid.ServerSchemaStatVisitor;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.util.SplitUtil;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlLockTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlLockTableStatement.LockType;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.List;

/**
 * lock tables [table] [write|read]
 *
 * @author songdabin
 */
public class DruidLockTableParser extends DefaultDruidParser {
    @Override
    public SchemaConfig visitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, ServerSchemaStatVisitor visitor, ServerConnection sc)
            throws SQLException {
        // for lock tables table1 write, table2
        // DruidParser can only parser table1,
        // use "," to judge
        String sql = rrs.getStatement();
        sql = sql.replaceAll("\n", " ").replaceAll("\t", " ");
        String[] stmts = SplitUtil.split(sql, ',', true);
        // contains ","
        if (stmts.length > 1) {
            String tmpStmt = null;
            String[] tmpWords = null;
            for (int i = 1; i < stmts.length; i++) {
                tmpStmt = stmts[i];
                tmpWords = SplitUtil.split(tmpStmt, ' ', true);
                if (tmpWords.length == 2 &&
                        ("READ".equalsIgnoreCase(tmpWords[1]) || "WRITE".equalsIgnoreCase(tmpWords[1]))) {
                    // unsupport lock multi-table
                    continue;
                } else {
                    // unsupport lock multi-table
                    throw new SQLNonTransientException(
                            "You have an error in your SQL syntax, don't support lock multi tables!");
                }
            }
            LOGGER.info("can't lock multi-table");
            throw new SQLNonTransientException("can't lock multi-table");
        }
        MySqlLockTableStatement lockTableStat = (MySqlLockTableStatement) stmt;
        String schemaName = schema == null ? null : schema.getName();
        SchemaUtil.SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(sc.getUser(), schemaName, lockTableStat.getTableSource());
        schema = schemaInfo.getSchemaConfig();
        String table = schemaInfo.getTable();
        if (RouterUtil.isNoSharding(schema, table)) {
            RouterUtil.routeToSingleNode(rrs, schema.getDataNode());
            rrs.setFinishedRoute(true);
            return schema;
        }
        TableConfig tableConfig = schema.getTables().get(table);
        if (tableConfig == null) {
            String msg = "can't find table define of " + table + " in schema:" + schema.getName();
            LOGGER.info(msg);
            throw new SQLNonTransientException(msg);
        }
        LockType lockType = lockTableStat.getLockType();
        if (LockType.WRITE != lockType && LockType.READ != lockType) {
            String msg = "lock type must be write or read";
            LOGGER.info(msg);
            throw new SQLNonTransientException(msg);
        }
        List<String> dataNodes = tableConfig.getDataNodes();
        RouteResultsetNode[] nodes = new RouteResultsetNode[dataNodes.size()];
        for (int i = 0; i < dataNodes.size(); i++) {
            nodes[i] = new RouteResultsetNode(dataNodes.get(i), ServerParse.LOCK, statementToString(stmt));
        }
        rrs.setNodes(nodes);
        rrs.setFinishedRoute(true);
        return schema;
    }
}
