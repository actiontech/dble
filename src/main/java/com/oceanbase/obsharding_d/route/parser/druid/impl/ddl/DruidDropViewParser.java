/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.route.parser.druid.impl.ddl;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.ExecutableHandler;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.ddl.ImplicitlyCommitCallback;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.ddl.DDLHandlerBuilder;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.config.model.sharding.SchemaConfig;
import com.oceanbase.obsharding_d.meta.ProxyMetaManager;
import com.oceanbase.obsharding_d.meta.ViewMeta;
import com.oceanbase.obsharding_d.plan.node.TableNode;
import com.oceanbase.obsharding_d.route.RouteResultset;
import com.oceanbase.obsharding_d.route.RouteResultsetNode;
import com.oceanbase.obsharding_d.route.parser.druid.ServerSchemaStatVisitor;
import com.oceanbase.obsharding_d.route.parser.druid.impl.DruidImplicitCommitParser;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;
import com.oceanbase.obsharding_d.singleton.ProxyMeta;
import com.oceanbase.obsharding_d.util.CollectionUtil;
import com.oceanbase.obsharding_d.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropViewStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class DruidDropViewParser extends DruidImplicitCommitParser {

    @Override
    public SchemaConfig doVisitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, ServerSchemaStatVisitor visitor, ShardingService service, boolean isExplain) throws SQLException {
        SQLDropViewStatement dropViewStatement = (SQLDropViewStatement) stmt;
        boolean ifExistsFlag = dropViewStatement.isIfExists();
        ProxyMetaManager proxyManger = ProxyMeta.getInstance().getTmManager();
        String vSchema, defaultSchema = null, nodeName = null;
        Map<String, Set<String>> deleteMysqlViewMaps = new HashMap<>(4);
        if (schema != null) {
            defaultSchema = schema.getName();
        }
        //need checkView
        checkView(defaultSchema, dropViewStatement);
        for (SQLExprTableSource table : dropViewStatement.getTableSources()) {
            vSchema = table.getSchema() == null ? defaultSchema : StringUtil.removeBackQuote(table.getSchema());
            String viewName = StringUtil.removeBackQuote(table.getName().getSimpleName()).trim();
            proxyManger.addMetaLock(vSchema, viewName, rrs.getStatement());
            try {
                proxyManger.getRepository().delete(vSchema, viewName);
                ViewMeta vm = proxyManger.getCatalogs().get(vSchema).getViewMetas().remove(viewName);
                if (vm != null && vm.getViewQuery() instanceof TableNode) {
                    nodeName = OBsharding_DServer.getInstance().getConfig().getSchemas().get(vSchema).getDefaultSingleNode();
                    if (!deleteMysqlViewMaps.containsKey(nodeName)) {
                        deleteMysqlViewMaps.put(nodeName, new HashSet<>(4));
                    }
                    deleteMysqlViewMaps.get(nodeName).add("`" + viewName + "`");
                }
            } finally {
                proxyManger.removeMetaLock(vSchema, viewName);
            }
        }

        if (!CollectionUtil.isEmpty(deleteMysqlViewMaps)) {
            StringBuilder dropStmt;
            List<RouteResultsetNode> nodes = new ArrayList<>();
            for (Map.Entry<String, Set<String>> n : deleteMysqlViewMaps.entrySet()) {
                dropStmt = new StringBuilder("drop view ");
                if (ifExistsFlag) {
                    dropStmt.append("if exists ");
                }
                dropStmt.append(n.getValue().stream().collect(Collectors.joining(",")));
                nodes.add(new RouteResultsetNode(n.getKey(), rrs.getSqlType(), dropStmt.toString()));
            }
            rrs.setNodes(nodes.toArray(new RouteResultsetNode[nodes.size()]));
            rrs.setFinishedRoute(true);
        } else {
            rrs.setFinishedExecute(true);
        }
        return schema;
    }

    @Override
    public ExecutableHandler visitorParseEnd(RouteResultset rrs, ShardingService service, ImplicitlyCommitCallback implicitlyCommitCallback) {
        return DDLHandlerBuilder.buildView(service.getSession2(), rrs, null, implicitlyCommitCallback);
    }

    private void checkView(String defaultSchema, SQLDropViewStatement dropViewStatement) throws SQLException {
        String vSchema;
        ProxyMetaManager proxyManger = ProxyMeta.getInstance().getTmManager();
        boolean ifExistsFlag = dropViewStatement.isIfExists();
        for (SQLExprTableSource table : dropViewStatement.getTableSources()) {
            vSchema = table.getSchema() == null ? defaultSchema : StringUtil.removeBackQuote(table.getSchema());
            checkSchema(vSchema);
            String viewName = StringUtil.removeBackQuote(table.getName().getSimpleName()).trim();
            if (proxyManger.getCatalogs().get(vSchema) == null) {
                throw new SQLException("Unknown database " + vSchema, "42000", ErrorCode.ER_BAD_DB_ERROR);
            }
            if (!proxyManger.getCatalogs().get(vSchema).getViewMetas().containsKey(viewName) && !ifExistsFlag) {
                throw new SQLException("Unknown view '" + viewName + "'", "HY000", ErrorCode.ER_NO_TABLES_USED);
            }
        }
    }
}
