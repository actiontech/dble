/*
 * Copyright (C) 2016-2019 ActionTech.
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
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlLockTableStatement;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.*;

/**
 * lock tables [table] [write|read]
 *
 * @author songdabin
 */
public class DruidLockTableParser extends DefaultDruidParser {
    @Override
    public SchemaConfig visitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, ServerSchemaStatVisitor visitor, ServerConnection sc, boolean isExplain)
            throws SQLException {
        MySqlLockTableStatement lockTableStat = (MySqlLockTableStatement) stmt;
        Map<String, List<String>> dataNodeToLocks = new HashMap<>();
        for (MySqlLockTableStatement.Item item : lockTableStat.getItems()) {
            String schemaName = schema == null ? null : schema.getName();
            SchemaUtil.SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(sc.getUser(), schemaName, item.getTableSource());
            SchemaConfig schemaConfig = schemaInfo.getSchemaConfig();
            String table = schemaInfo.getTable();
            String noShardingNode = RouterUtil.isNoShardingDDL(schemaConfig, table);
            if (noShardingNode != null) {
                StringBuilder sbItem = new StringBuilder(table);
                if (item.getTableSource().getAlias() != null) {
                    sbItem.append(" as ");
                    sbItem.append(item.getTableSource().getAlias());
                }
                sbItem.append(" ");
                sbItem.append(item.getLockType());
                List<String> locks = dataNodeToLocks.computeIfAbsent(noShardingNode, k -> new ArrayList<>());
                locks.add(sbItem.toString());
                continue;
            }
            TableConfig tableConfig = schemaConfig.getTables().get(table);
            if (tableConfig == null) {
                String msg = "can't find table define of " + table + " in schema:" + schemaConfig.getName();
                LOGGER.info(msg);
                throw new SQLNonTransientException(msg);
            }
            List<String> dataNodes = tableConfig.getDataNodes();
            for (String dataNode : dataNodes) {
                StringBuilder sbItem = new StringBuilder(table);
                if (item.getTableSource().getAlias() != null) {
                    sbItem.append(" as ");
                    sbItem.append(item.getTableSource().getAlias());
                }
                sbItem.append(" ");
                sbItem.append(item.getLockType());
                List<String> locks = dataNodeToLocks.computeIfAbsent(dataNode, k -> new ArrayList<>());
                locks.add(sbItem.toString());
            }
        }
        Set<RouteResultsetNode> lockedNodes = new HashSet<>();
        if (sc.isLocked()) {
            lockedNodes.addAll(sc.getSession2().getTargetMap().keySet());
        }
        List<RouteResultsetNode> nodes = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : dataNodeToLocks.entrySet()) {
            RouteResultsetNode node = new RouteResultsetNode(entry.getKey(), ServerParse.LOCK, " LOCK TABLES " + StringUtil.join(entry.getValue(), ","));
            nodes.add(node);
            lockedNodes.remove(node);
        }
        for (RouteResultsetNode toUnlockedNode : lockedNodes) {
            RouteResultsetNode node = new RouteResultsetNode(toUnlockedNode.getName(), ServerParse.UNLOCK, " UNLOCK TABLES ");
            nodes.add(node);
        }
        rrs.setNodes(nodes.toArray(new RouteResultsetNode[nodes.size()]));
        rrs.setFinishedRoute(true);
        return schema;
    }
}
