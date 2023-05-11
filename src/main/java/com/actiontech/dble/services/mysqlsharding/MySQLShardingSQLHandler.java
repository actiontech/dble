/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.mysqlsharding;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.user.ShardingUserConfig;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.server.response.ShowCreateView;
import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.singleton.ProxyMeta;
import com.actiontech.dble.singleton.RouteService;
import com.actiontech.dble.singleton.TraceManager;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLNonTransientException;

/**
 * Created by szf on 2020/7/3.
 */
public class MySQLShardingSQLHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLShardingSQLHandler.class);

    private final ShardingService service;

    MySQLShardingSQLHandler(ShardingService service) {
        this.service = service;
    }

    public void routeEndExecuteSQL(String sql, int type, SchemaConfig schemaConfig) {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(service, "route&execute");
        try {
            if (service.getSession2().isKilled()) {
                LOGGER.info("{} sql[{}] is killed.", service.toString2(), service.getExecuteSql());
                service.writeErrMessage(ErrorCode.ER_QUERY_INTERRUPTED, "The query is interrupted.");
                return;
            }

            RouteResultset rrs;
            try {
                rrs = RouteService.getInstance().route(schemaConfig, type, sql, service);
                if (rrs == null) {
                    return;
                }
                if (rrs.getSqlType() == ServerParse.DDL && rrs.getSchema() != null) {
                    if (ProxyMeta.getInstance().getTmManager().getCatalogs().get(rrs.getSchema()).getView(rrs.getTable()) != null) {
                        ProxyMeta.getInstance().getTmManager().removeMetaLock(rrs.getSchema(), rrs.getTable());
                        String msg = "Table '" + rrs.getTable() + "' already exists as a view";
                        LOGGER.info(msg);
                        throw new SQLNonTransientException(msg);
                    }
                }
            } catch (Exception e) {
                service.executeException(e, sql);
                return;
            }
            service.getSession2().trace(t -> t.endRoute(rrs));
            service.getSession2().execute(rrs);
        } finally {
            TraceManager.finishSpan(traceObject);
        }
    }

    public void routeSystemInfoAndExecuteSQL(String stmt, SchemaUtil.SchemaInfo schemaInfo, int sqlType) {
        ShardingUserConfig user = (ShardingUserConfig) (DbleServer.getInstance().getConfig().getUsers().get(service.getUser()));
        if (user == null || !user.getSchemas().contains(schemaInfo.getSchema())) {
            service.writeErrMessage("42000", "Access denied for user '" + service.getUser().getFullName() + "' to database '" + schemaInfo.getSchema() + "'", ErrorCode.ER_DBACCESS_DENIED_ERROR);
            return;
        }
        RouteResultset rrs = new RouteResultset(stmt, sqlType);
        try {
            String noShardingNode = RouterUtil.isNoSharding(schemaInfo.getSchemaConfig(), schemaInfo.getTable());
            if (noShardingNode != null) {
                RouterUtil.routeToSingleNode(rrs, noShardingNode, Sets.newHashSet(schemaInfo.getSchema() + "." + schemaInfo.getTable()));
            } else {
                if (schemaInfo.getSchemaConfig().getTables().get(schemaInfo.getTable()) == null) {
                    // check view
                    ShowCreateView.response(service, schemaInfo.getSchema(), schemaInfo.getTable());
                    return;
                }
                RouterUtil.routeToRandomNode(rrs, schemaInfo.getSchemaConfig(), schemaInfo.getTable());
            }
            service.getSession2().execute(rrs);
        } catch (Exception e) {
            service.executeException(e, stmt);
        }
    }
}
