/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.parser.druid.impl.ddl;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.nio.handler.ExecutableHandler;
import com.actiontech.dble.backend.mysql.nio.handler.ddl.ImplicitlyCommitCallback;
import com.actiontech.dble.backend.mysql.nio.handler.ddl.DDLHandlerBuilder;
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
import com.alibaba.druid.sql.ast.statement.SQLCreateViewStatement;

import java.sql.SQLException;

public class DruidCreateOrReplaceViewParser extends DruidImplicitCommitParser {
    Boolean isCreate = null;
    ViewMeta vm = null;

    @Override
    public SchemaConfig doVisitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, ServerSchemaStatVisitor visitor, ShardingService service, boolean isExplain) throws SQLException {
        String sql = rrs.getStatement();
        vm = new ViewMeta(schema.getName(), sql, ProxyMeta.getInstance().getTmManager());
        vm.init();
        SQLCreateViewStatement createViewStatement = (SQLCreateViewStatement) stmt;
        checkSchema(vm.getSchema());
        if (createViewStatement.isOrReplace()) {
            PlanNode oldViewNode = ProxyMeta.getInstance().getTmManager().getSyncView(vm.getSchema(), vm.getViewName());
            if (oldViewNode instanceof TableNode && vm.getViewQuery() instanceof QueryNode) {
                rrs.setStatement("drop view `" + vm.getViewName() + "`");
                rrs.setSchema(vm.getSchema());
                RouterUtil.routeToSingleNode(rrs, DbleServer.getInstance().getConfig().getSchemas().get(vm.getSchema()).getDefaultSingleNode(), null);
                rrs.setFinishedRoute(true);
                isCreate = false;
            } else if (vm.getViewQuery() instanceof TableNode) {
                rrs.setStatement(RouterUtil.removeSchema(sql, vm.getSchema()));
                rrs.setSchema(vm.getSchema());
                RouterUtil.routeToSingleNode(rrs, DbleServer.getInstance().getConfig().getSchemas().get(vm.getSchema()).getDefaultSingleNode(), null);
                rrs.setFinishedRoute(true);
                isCreate = true;
            } else {
                vm.addMeta(true);
                rrs.setFinishedExecute(true);
            }
        } else {
            if (vm.getViewQuery() instanceof TableNode) {
                rrs.setStatement(RouterUtil.removeSchema(sql, vm.getSchema()));
                rrs.setSchema(vm.getSchema());
                RouterUtil.routeToSingleNode(rrs, DbleServer.getInstance().getConfig().getSchemas().get(vm.getSchema()).getDefaultSingleNode(), null);
                rrs.setFinishedRoute(true);
                isCreate = true;
            } else {
                vm.addMeta(true);
                rrs.setFinishedExecute(true);
            }
        }
        return schema;
    }

    public ExecutableHandler visitorParseEnd(RouteResultset rrs, ShardingService service, ImplicitlyCommitCallback implicitlyCommitCallback) {
        if (null != isCreate) {
            return DDLHandlerBuilder.buildView(service.getSession2(), rrs, vm, implicitlyCommitCallback);
        }
        return null;
    }
}
