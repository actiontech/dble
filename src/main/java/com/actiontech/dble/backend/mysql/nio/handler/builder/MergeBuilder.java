/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.builder;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.plan.node.PlanNode;
import com.actiontech.dble.plan.node.TableNode;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.parser.druid.ServerSchemaStatVisitor;
import com.actiontech.dble.route.parser.druid.impl.DruidSingleUnitSelectParser;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.server.NonBlockingSession;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class MergeBuilder {
    private PlanNode node;
    private NonBlockingSession session;
    private Map<String, SchemaConfig> schemaConfigMap = new HashMap<>();

    MergeBuilder(NonBlockingSession session, PlanNode node) {
        this.node = node;
        this.session = session;
        this.schemaConfigMap.putAll(DbleServer.getInstance().getConfig().getSchemas());
    }

    RouteResultset constructByStatement(RouteResultset rrs, SQLSelectStatement select, SchemaConfig schemaConfig) throws SQLException {
        Map<Pair<String, String>, SchemaConfig> tableConfigMap = new HashMap<>();
        for (TableNode tn : node.getReferedTableNodes()) {
            if (schemaConfigMap.get(tn.getSchema()) != null) {
                tableConfigMap.put(new Pair<>(tn.getSchema(), tn.getTableName()), schemaConfigMap.get(tn.getSchema()));
            }
        }
        DruidSingleUnitSelectParser druidParser = new DruidSingleUnitSelectParser();
        druidParser.setSchemaMap(tableConfigMap);
        return RouterUtil.routeFromParserComplex(schemaConfig, druidParser, tableConfigMap, rrs, select, new ServerSchemaStatVisitor(schemaConfig.getName()), session.getShardingService());
    }


}
