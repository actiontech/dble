/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.information;

import com.actiontech.dble.plan.node.PlanNode;
import com.actiontech.dble.plan.node.QueryNode;
import com.actiontech.dble.plan.visitor.MySQLPlanNodeVisitor;
import com.actiontech.dble.route.factory.RouteStrategyFactory;
import com.actiontech.dble.services.manager.information.tables.*;
import com.actiontech.dble.services.manager.information.tables.statistic.AssociateTablesByEntryByUser;
import com.actiontech.dble.services.manager.information.tables.statistic.FrontendByBackendByEntryByUser;
import com.actiontech.dble.services.manager.information.tables.statistic.SqlLog;
import com.actiontech.dble.services.manager.information.tables.statistic.TableByUserByEntry;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;

import java.sql.SQLSyntaxErrorException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public final class ManagerSchemaInfo {
    public static final String SCHEMA_NAME = "dble_information";
    private static final ManagerSchemaInfo INSTANCE = new ManagerSchemaInfo();

    private Map<String, ManagerBaseTable> tables = new TreeMap<>();
    private Map<String, QueryNode> views;

    private ManagerSchemaInfo() {
        registerTable(new Version());
        registerTable(new DbleVariables());
        registerTable(new DbleThreadPool());
        registerTable(new DbleFrontConnections());
        registerTable(new DbleBackendConnections());
        registerTable(new DbleShardingNode());
        registerTable(new DbleSchema());
        registerTable(new DbleThreadUsage());
        registerTable(new DbleEntry());
        registerTable(new DbleEntrySchema());
        registerTable(new DbleEntryDbGroup());
        registerTable(new DbleRwSplitEntry());
        registerTable(new DbleEntryTablePrivilege());
        registerTable(new DbleStatus());
        registerTable(new DbleProcessor());
        registerTable(new DbleBlacklist());
        registerTable(new DbleReloadStatus());
        registerTable(new DbleXaSession());
        registerTable(new DbleDdlLock());
        registerTable(new DbleTable());
        registerTable(new DbleGlobalTable());
        registerTable(new DbleShardingTable());
        registerTable(new DbleChildTable());
        registerTable(new DbleTableShardingNode());
        registerTable(new DbleDbGroup());
        registerTable(new DbleDbInstance());
        registerTable(new DbleAlgorithm());
        registerTable(new ProcessList());
        registerTable(new SessionVariables());
        registerTable(new BackendVariables());
        registerTable(new DbleConfig());
        registerTable(new FrontendByBackendByEntryByUser());
        registerTable(new TableByUserByEntry());
        registerTable(new AssociateTablesByEntryByUser());
        // sampling
        registerTable(new SqlLog());
        registerTable(new DbleXaRecover());
    }

    private void registerTable(ManagerBaseTable table) {
        tables.put(table.getTableName(), table);
    }

    private void registerView(String viewName, String alias, String selectSql) throws SQLSyntaxErrorException {
        SQLSelectStatement selectStatement = (SQLSelectStatement) RouteStrategyFactory.getRouteStrategy().parserSQL(selectSql);
        MySQLPlanNodeVisitor msv = new MySQLPlanNodeVisitor(SCHEMA_NAME, 45, null, false, null);
        msv.visit(selectStatement.getSelect().getQuery());
        PlanNode selNode = msv.getTableNode();
        selNode.setUpFields();
        QueryNode queryNode = new QueryNode(selNode);
        queryNode.setAlias(alias);
        views.put(viewName, queryNode);
    }

    public static ManagerSchemaInfo getInstance() {
        return INSTANCE;
    }

    public Map<String, ManagerBaseTable> getTables() {
        return tables;
    }

    public QueryNode getView(String viewName) {
        if (views == null) {
            views = new HashMap<>(8);
            try {
                registerView("sql_log_by_tx_by_entry_by_user", "s1", "select tx_id,entry,user,source_host,source_port,GROUP_CONCAT(sql_id) sql_ids, COUNT(sql_id) sql_count,max(start_time + duration) - min(start_time) tx_duration,sum(duration) busy_time,sum(examined_rows) examined_rows from sql_log group by tx_id");
                registerView("sql_log_by_digest_by_entry_by_user", "s2", "select sql_digest,entry,user,COUNT(sql_id) exec,sum(duration) duration,sum(rows) rows,sum(examined_rows) examined_rows from sql_log group by sql_digest");
                registerView("sql_log_by_tx_digest_by_entry_by_user", "s3", "select tx_digest,count(tx_digest) exec, user,entry,sum(sql_count) sql_count,source_host,source_port,group_concat(sql_ids) sql_ids,sum(tx_duration) tx_duration,sum(busy_time) busy_time,sum(examined_rows) examined_rows from (select group_concat(sql_digest) tx_digest,user,entry,COUNT(sql_id) sql_count,source_host,source_port,GROUP_CONCAT(sql_id) sql_ids,max(start_time + duration) - min(start_time) tx_duration,sum(duration) busy_time,sum(examined_rows) examined_rows from sql_log group by tx_id) a group by a.tx_digest");
            } catch (SQLSyntaxErrorException e) {
                return null;
            }
        }

        return views.get(viewName);
    }
}
