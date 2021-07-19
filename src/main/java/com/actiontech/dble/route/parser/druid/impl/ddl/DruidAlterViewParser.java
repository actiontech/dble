package com.actiontech.dble.route.parser.druid.impl.ddl;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.nio.handler.ExecutableHandler;
import com.actiontech.dble.backend.mysql.nio.handler.MysqlCreateViewHandler;
import com.actiontech.dble.backend.mysql.nio.handler.MysqlDropViewHandler;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.meta.ViewMeta;
import com.actiontech.dble.plan.node.PlanNode;
import com.actiontech.dble.plan.node.QueryNode;
import com.actiontech.dble.plan.node.TableNode;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.parser.druid.ServerSchemaStatVisitor;
import com.actiontech.dble.route.parser.druid.impl.DruidImplicitCommitParser;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.singleton.ProxyMeta;
import com.alibaba.druid.sql.ast.SQLStatement;

import java.sql.SQLException;

public class DruidAlterViewParser extends DruidImplicitCommitParser {
    ViewMeta vm = null;
    Boolean isCreate = null;

    @Override
    public SchemaConfig doVisitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, ServerSchemaStatVisitor visitor, ShardingService service, boolean isExplain) throws SQLException {
        String sql = rrs.getStatement();
        vm = new ViewMeta(schema.getName(), sql, ProxyMeta.getInstance().getTmManager());
        vm.init();
        checkSchema(vm.getSchema());
        PlanNode oldViewNode = ProxyMeta.getInstance().getTmManager().getSyncView(vm.getSchema(), vm.getViewName());
        if (oldViewNode instanceof TableNode && vm.getViewQuery() instanceof QueryNode) {
            rrs.setStatement("drop view `" + vm.getViewName() + "`");
            rrs.setSchema(vm.getSchema());
            RouterUtil.routeToSingleNode(rrs, DbleServer.getInstance().getConfig().getSchemas().get(vm.getSchema()).getShardingNode(), null);
            rrs.setFinishedRoute(true);
            isCreate = false;
        } else if (vm.getViewQuery() instanceof TableNode) {
            rrs.setStatement(RouterUtil.removeSchema(sql, vm.getSchema()));
            rrs.setSchema(vm.getSchema());
            RouterUtil.routeToSingleNode(rrs, DbleServer.getInstance().getConfig().getSchemas().get(vm.getSchema()).getShardingNode(), null);
            rrs.setFinishedRoute(true);
            isCreate = true;
        } else {
            vm.addMeta(true);
            rrs.setFinishedExecute(true);
        }
        return schema;
    }

    public ExecutableHandler visitorParseEnd(RouteResultset rrs, ShardingService service) {
        if (null != isCreate) {
            if (isCreate) {
                return new MysqlCreateViewHandler(service.getSession2(), rrs, vm);
            } else {
                return new MysqlDropViewHandler(service.getSession2(), rrs, vm);
            }
        }
        return null;
    }
}
