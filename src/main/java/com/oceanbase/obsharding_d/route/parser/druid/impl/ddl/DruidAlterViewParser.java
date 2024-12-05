/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.route.parser.druid.impl.ddl;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.ExecutableHandler;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.ddl.ImplicitlyCommitCallback;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.ddl.DDLHandlerBuilder;
import com.oceanbase.obsharding_d.config.model.sharding.SchemaConfig;
import com.oceanbase.obsharding_d.meta.ViewMeta;
import com.oceanbase.obsharding_d.plan.node.PlanNode;
import com.oceanbase.obsharding_d.plan.node.QueryNode;
import com.oceanbase.obsharding_d.plan.node.TableNode;
import com.oceanbase.obsharding_d.route.RouteResultset;
import com.oceanbase.obsharding_d.route.parser.druid.ServerSchemaStatVisitor;
import com.oceanbase.obsharding_d.route.parser.druid.impl.DruidImplicitCommitParser;
import com.oceanbase.obsharding_d.route.util.RouterUtil;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;
import com.oceanbase.obsharding_d.singleton.ProxyMeta;
import com.alibaba.druid.sql.ast.SQLStatement;

import java.sql.SQLException;

public class DruidAlterViewParser extends DruidImplicitCommitParser {
    ViewMeta vm = null;
    Boolean isCreate = null;

    @Override
    public SchemaConfig doVisitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, ServerSchemaStatVisitor visitor, ShardingService service, boolean isExplain) throws SQLException {
        String sql = rrs.getStatement();
        String schemaName = schema == null ? null : schema.getName();
        vm = new ViewMeta(schemaName, sql, ProxyMeta.getInstance().getTmManager());
        vm.init();
        checkSchema(vm.getSchema());
        PlanNode oldViewNode = ProxyMeta.getInstance().getTmManager().getSyncView(vm.getSchema(), vm.getViewName());
        if (oldViewNode instanceof TableNode && vm.getViewQuery() instanceof QueryNode) {
            rrs.setStatement("drop view `" + vm.getViewName() + "`");
            rrs.setSchema(vm.getSchema());
            RouterUtil.routeToSingleNode(rrs, OBsharding_DServer.getInstance().getConfig().getSchemas().get(vm.getSchema()).getDefaultSingleNode(), null);
            rrs.setFinishedRoute(true);
            isCreate = false;
        } else if (vm.getViewQuery() instanceof TableNode) {
            rrs.setStatement(RouterUtil.removeSchema(sql, vm.getSchema()));
            rrs.setSchema(vm.getSchema());
            RouterUtil.routeToSingleNode(rrs, OBsharding_DServer.getInstance().getConfig().getSchemas().get(vm.getSchema()).getDefaultSingleNode(), null);
            rrs.setFinishedRoute(true);
            isCreate = true;
        } else {
            vm.addMeta(true);
            rrs.setFinishedExecute(true);
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
