package com.actiontech.dble.route.parser.druid.impl.ddl;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.nio.handler.ExecutableHandler;
import com.actiontech.dble.backend.mysql.nio.handler.MysqlDropViewHandler;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.meta.ProxyMetaManager;
import com.actiontech.dble.meta.ViewMeta;
import com.actiontech.dble.plan.node.TableNode;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.route.parser.druid.ServerSchemaStatVisitor;
import com.actiontech.dble.route.parser.druid.impl.DruidImplicitCommitParser;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.singleton.ProxyMeta;
import com.actiontech.dble.util.CollectionUtil;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropViewStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class DruidDropViewParser extends DruidImplicitCommitParser {
    int viewNum = 0;

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
            proxyManger.addMetaLock(vSchema, viewName, rrs.getStatement());
            try {
                proxyManger.getRepository().delete(vSchema, viewName);
                ViewMeta vm = proxyManger.getCatalogs().get(vSchema).getViewMetas().remove(viewName);
                if (vm != null && vm.getViewQuery() instanceof TableNode) {
                    nodeName = DbleServer.getInstance().getConfig().getSchemas().get(vSchema).getShardingNode();
                    if (!deleteMysqlViewMaps.containsKey(nodeName)) {
                        deleteMysqlViewMaps.put(nodeName, new HashSet<>(4));
                    }
                    deleteMysqlViewMaps.get(nodeName).add(viewName);
                }
            } finally {
                proxyManger.removeMetaLock(vSchema, viewName);
            }
        }

        if (!CollectionUtil.isEmpty(deleteMysqlViewMaps)) {
            StringBuilder dropStmt;
            List<RouteResultsetNode> nodes = new ArrayList<>();
            for (Map.Entry<String, Set<String>> n : deleteMysqlViewMaps.entrySet()) {
                viewNum += n.getValue().size();
                dropStmt = new StringBuilder("drop view ");
                if (ifExistsFlag) {
                    dropStmt.append("if exists ");
                }
                dropStmt.append(n.getValue().stream().collect(Collectors.joining(",")));
                nodes.add(new RouteResultsetNode(nodeName, rrs.getSqlType(), dropStmt.toString()));
            }
            rrs.setNodes(nodes.toArray(new RouteResultsetNode[nodes.size()]));
            rrs.setFinishedRoute(true);
        } else {
            rrs.setFinishedExecute(true);
        }
        return schema;
    }

    @Override
    public ExecutableHandler visitorParseEnd(RouteResultset rrs, ShardingService service) {
        return new MysqlDropViewHandler(service.getSession2(), rrs, viewNum, null);
    }
}
