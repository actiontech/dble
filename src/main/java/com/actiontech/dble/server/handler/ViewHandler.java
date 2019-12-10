/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.nio.handler.MysqlViewHandler;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.meta.ProxyMetaManager;
import com.actiontech.dble.meta.ViewMeta;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.singleton.ProxyMeta;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.statement.SQLDropViewStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;

/**
 * Created by collapsar on 2019/12/10.
 */
public final class ViewHandler {

    private ViewHandler() {
    }

    public static void handle(int type, String sql, ServerConnection c) {
        String schema = c.getSchema();
        if (StringUtil.isEmpty(schema)) {
            c.writeErrMessage("3D000", "No database selected", ErrorCode.ER_NO_DB_ERROR);
            return;
        }

        try {
            // view in mysql
            SchemaConfig schemaConfig = DbleServer.getInstance().getConfig().getSchemas().get(schema);
            if (schemaConfig.isNoSharding()) {
                RouteResultset rrs = new RouteResultset(RouterUtil.removeSchema(sql, schema), type);
                rrs.setSchema(schema);
                RouterUtil.routeToSingleNode(rrs, schemaConfig.getDataNode());
                MysqlViewHandler handler = new MysqlViewHandler(c.getSession2(), rrs);
                handler.execute();
                return;
            }

            // view in dble
            handleView(type, schema, sql, false);
        } catch (Exception e) {
            c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, e.getMessage());
            return;
        }

        // if the create success with no error send back OK
        byte packetId = (byte) c.getSession2().getPacketId().get();
        OkPacket ok = new OkPacket();
        ok.setPacketId(++packetId);
        c.getSession2().multiStatementPacket(ok, packetId);
        ok.write(c);
        boolean multiStatementFlag = c.getSession2().getIsMultiStatement().get();
        c.getSession2().multiStatementNextSql(multiStatementFlag);
    }

    public static void handleView(int sqlType, String schema, String sql, boolean isMysqlView) throws Exception {
        switch (sqlType) {
            case ServerParse.CREATE_VIEW:
            case ServerParse.ALTER_VIEW:
                createOrReplaceView(schema, sql, false, isMysqlView);
                break;
            case ServerParse.REPLACE_VIEW:
                createOrReplaceView(schema, sql, true, isMysqlView);
                break;
            case ServerParse.DROP_VIEW:
                deleteView(schema, sql, isMysqlView);
                break;
            default:
                break;
        }
    }

    /**
     * createOrReplaceView
     *
     * @param schema
     * @param sql
     * @param isReplace
     * @throws Exception
     */
    private static void createOrReplaceView(String schema, String sql, boolean isReplace, boolean isMysqlView) throws Exception {
        //create a new object of the view
        ViewMeta vm = new ViewMeta(schema, sql, ProxyMeta.getInstance().getTmManager());
        vm.initAndSet(isReplace, true, isMysqlView);
    }

    /**
     * delete view
     *
     * @param currentSchema
     * @param sql
     * @throws Exception
     */
    private static void deleteView(String currentSchema, String sql, boolean isMysqlView) throws Exception {
        SQLStatementParser parser = new MySqlStatementParser(sql);
        SQLDropViewStatement viewStatement = (SQLDropViewStatement) parser.parseStatement(true);
        if (viewStatement.getTableSources() == null || viewStatement.getTableSources().size() == 0) {
            throw new Exception("no view in sql when try to drop view.");
        }

        boolean ifExistsFlag = viewStatement.isIfExists();
        ProxyMetaManager proxyManger = ProxyMeta.getInstance().getTmManager();
        for (SQLExprTableSource table : viewStatement.getTableSources()) {
            String schema = table.getSchema() == null ? currentSchema : StringUtil.removeBackQuote(table.getSchema());
            String viewName = StringUtil.removeBackQuote(table.getName().getSimpleName()).trim();

            if (!(proxyManger.getCatalogs().get(schema).getViewMetas().containsKey(viewName) && !ifExistsFlag)) {
                throw new Exception("Unknown view '" + table.getName().toString() + "'");
            }

            proxyManger.addMetaLock(table.getSchema(), viewName, sql);
            try {
                if (!isMysqlView) proxyManger.getRepository().delete(schema, viewName);
                proxyManger.getCatalogs().get(schema).getViewMetas().remove(viewName);
            } finally {
                proxyManger.removeMetaLock(table.getSchema(), viewName);
            }
        }
    }
}
