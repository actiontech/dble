/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.nio.handler.builder;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.config.model.sharding.SchemaConfig;
import com.oceanbase.obsharding_d.plan.node.PlanNode;
import com.oceanbase.obsharding_d.plan.node.TableNode;
import com.oceanbase.obsharding_d.route.RouteResultset;
import com.oceanbase.obsharding_d.route.parser.druid.ServerSchemaStatVisitor;
import com.oceanbase.obsharding_d.route.parser.druid.impl.DruidSingleUnitSelectParser;
import com.oceanbase.obsharding_d.route.parser.util.Pair;
import com.oceanbase.obsharding_d.route.util.RouterUtil;
import com.oceanbase.obsharding_d.server.NonBlockingSession;
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
        this.schemaConfigMap.putAll(OBsharding_DServer.getInstance().getConfig().getSchemas());
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
