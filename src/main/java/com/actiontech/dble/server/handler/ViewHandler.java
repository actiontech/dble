/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.nio.handler.MysqlCreateViewHandler;
import com.actiontech.dble.backend.mysql.nio.handler.MysqlDropViewHandler;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.meta.ProxyMetaManager;
import com.actiontech.dble.meta.ViewMeta;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.plan.node.PlanNode;
import com.actiontech.dble.plan.node.QueryNode;
import com.actiontech.dble.plan.node.TableNode;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.util.RouterUtil;

import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.singleton.ProxyMeta;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.statement.SQLDropViewStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by collapsar on 2019/12/10.
 */
public final class ViewHandler {

    private ViewHandler() {
    }

    public static void handle(int type, String sql, ShardingService service) {
        String schema = service.getSchema();
        try {
            handleView(type, schema, sql, service);
        } catch (SQLException e) {
            service.writeErrMessage(e.getSQLState(), (e.getMessage() == null ? e.toString() : e.getMessage()), e.getErrorCode());
        } catch (Exception e) {
            service.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, (e.getMessage() == null ? e.toString() : e.getMessage()));
        }
    }

    public static void handleView(int sqlType, String schema, String sql, ShardingService service) throws Exception {
        switch (sqlType) {
            case ServerParse.CREATE_VIEW:
                createView(schema, sql, sqlType, service);
                break;
            case ServerParse.ALTER_VIEW:
                replaceView(schema, sql, sqlType, service);
                break;
            case ServerParse.REPLACE_VIEW:
                replaceView(schema, sql, sqlType, service);
                break;
            case ServerParse.DROP_VIEW:
                dropView(schema, sql, service);
                break;
            default:
                break;
        }
    }

    private static void createView(String schema, String sql, int sqlType, ShardingService service) throws Exception {
        //create a new object of the view
        ViewMeta vm = new ViewMeta(schema, sql, ProxyMeta.getInstance().getTmManager());
        vm.init();
        checkSchema(vm.getSchema());
        // if the sql can push down nosharding sharding
        if (vm.getViewQuery() instanceof TableNode) {
            RouteResultset rrs = new RouteResultset(RouterUtil.removeSchema(sql, vm.getSchema()), sqlType);
            rrs.setSchema(vm.getSchema());
            RouterUtil.routeToSingleNode(rrs, DbleServer.getInstance().getConfig().getSchemas().get(vm.getSchema()).getShardingNode());
            MysqlCreateViewHandler handler = new MysqlCreateViewHandler(service.getSession2(), rrs, vm);
            handler.execute();
            return;
        }
        vm.addMeta(true);
        writeOkPackage(service);
    }

    private static void replaceView(String schema, String sql, int sqlType, ShardingService service) throws Exception {
        //create a new object of the view
        ViewMeta vm = new ViewMeta(schema, sql, ProxyMeta.getInstance().getTmManager());
        vm.init();
        checkSchema(vm.getSchema());
        // if exist
        PlanNode oldViewNode = ProxyMeta.getInstance().getTmManager().getSyncView(vm.getSchema(), vm.getViewName());
        if (oldViewNode instanceof TableNode && vm.getViewQuery() instanceof QueryNode) {
            RouteResultset rrs = new RouteResultset("drop view `" + vm.getViewName() + "`", sqlType);
            rrs.setSchema(vm.getSchema());
            RouterUtil.routeToSingleNode(rrs, DbleServer.getInstance().getConfig().getSchemas().get(vm.getSchema()).getShardingNode());
            MysqlDropViewHandler handler = new MysqlDropViewHandler(service.getSession2(), rrs, 1);
            handler.setVm(vm);
            handler.execute();
            return;
        }

        if (vm.getViewQuery() instanceof TableNode) {
            RouteResultset rrs = new RouteResultset(RouterUtil.removeSchema(sql, vm.getSchema()), sqlType);
            rrs.setSchema(vm.getSchema());
            RouterUtil.routeToSingleNode(rrs, DbleServer.getInstance().getConfig().getSchemas().get(vm.getSchema()).getShardingNode());
            MysqlCreateViewHandler handler = new MysqlCreateViewHandler(service.getSession2(), rrs, vm);
            handler.execute();
            return;
        }
        vm.addMeta(true);
        writeOkPackage(service);
    }

    private static void dropView(String currentSchema, String sql, ShardingService service) throws Exception {
        SQLStatementParser parser = new MySqlStatementParser(sql);
        SQLDropViewStatement viewStatement = (SQLDropViewStatement) parser.parseStatement(true);
        if (viewStatement.getTableSources() == null || viewStatement.getTableSources().size() == 0) {
            throw new SQLException("no view in sql when try to drop view.", "HY000", ErrorCode.ER_VIEW_CHECK_FAILED);
        }

        boolean ifExistsFlag = viewStatement.isIfExists();
        ProxyMetaManager proxyManger = ProxyMeta.getInstance().getTmManager();

        List<String> deleteMysqlViews = new ArrayList<>(5);
        String vSchema = null;
        for (SQLExprTableSource table : viewStatement.getTableSources()) {
            vSchema = table.getSchema() == null ? currentSchema : StringUtil.removeBackQuote(table.getSchema());
            checkSchema(vSchema);
            String viewName = StringUtil.removeBackQuote(table.getName().getSimpleName()).trim();
            if (proxyManger.getCatalogs().get(vSchema) == null) {
                throw new SQLException("Unknown database " + vSchema, "42000", ErrorCode.ER_BAD_DB_ERROR);
            }
            if (!proxyManger.getCatalogs().get(vSchema).getViewMetas().containsKey(viewName) && !ifExistsFlag) {
                throw new SQLException("Unknown view '" + viewName + "'", "HY000", ErrorCode.ER_NO_TABLES_USED);
            }

            proxyManger.addMetaLock(vSchema, viewName, sql);
            try {
                proxyManger.getRepository().delete(vSchema, viewName);
                ViewMeta vm = proxyManger.getCatalogs().get(vSchema).getViewMetas().remove(viewName);
                if (vm != null && vm.getViewQuery() instanceof TableNode) {
                    deleteMysqlViews.add(viewName);
                }
            } finally {
                proxyManger.removeMetaLock(vSchema, viewName);
            }
        }

        if (deleteMysqlViews.size() > 0) {
            StringBuilder dropStmt = new StringBuilder("drop view ");
            if (ifExistsFlag) {
                dropStmt.append(" if exists ");
            }
            for (String table : deleteMysqlViews) {
                dropStmt.append("`");
                dropStmt.append(table);
                dropStmt.append("`,");
            }
            dropStmt.deleteCharAt(dropStmt.length() - 1);
            RouteResultset rrs = new RouteResultset(dropStmt.toString(), ServerParse.DROP_VIEW);
            RouterUtil.routeToSingleNode(rrs, DbleServer.getInstance().getConfig().getSchemas().get(vSchema).getShardingNode());
            MysqlDropViewHandler handler = new MysqlDropViewHandler(service.getSession2(), rrs, deleteMysqlViews.size());
            handler.execute();
            return;
        }
        writeOkPackage(service);
    }

    private static void writeOkPackage(ShardingService service) {
        // if the create success with no error send back OK
        byte packetId = (byte) service.getSession2().getPacketId().get();
        OkPacket ok = new OkPacket();
        ok.setPacketId(++packetId);
        service.getSession2().multiStatementPacket(ok, packetId);
        ok.write(service.getConnection());
    }

    private static void checkSchema(String schema) throws SQLException {
        if (StringUtil.isEmpty(schema)) {
            throw new SQLException("No database selected", "3D000", ErrorCode.ER_NO_DB_ERROR);
        }
    }
}
