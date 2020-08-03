/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.response;

import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.meta.SchemaMeta;
import com.actiontech.dble.meta.ViewMeta;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResultSetHeaderPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.route.factory.RouteStrategyFactory;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.singleton.ProxyMeta;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateViewStatement;

import java.nio.ByteBuffer;
import java.sql.SQLException;

/**
 * Created by szf on 2017/12/15.
 */
public final class ShowCreateView {


    private static final int FIELD_COUNT = 4;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("View", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("Create View", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("character_set_client", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("collation_connection", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i].setPacketId(++packetId);

        EOF.setPacketId(++packetId);
    }

    private ShowCreateView() {
    }

    public static void response(ServerConnection c, String stmt) {
        try {
            MySqlShowCreateViewStatement statement = (MySqlShowCreateViewStatement) RouteStrategyFactory.getRouteStrategy().parserSQL(stmt);
            String schema = null;
            String view = null;
            if (statement.getName() instanceof SQLPropertyExpr) {
                //show create view with sharding
                SQLPropertyExpr sqlPropertyExpr = (SQLPropertyExpr) statement.getName();
                //protocol not equals the nomul things
                schema = sqlPropertyExpr.getOwner().toString();
                view = sqlPropertyExpr.getName();
            } else if (statement.getName() instanceof SQLIdentifierExpr) {
                schema = c.getSchema();
                view = statement.getName().toString();
            }
            sendOutTheViewInfo(c, schema, view);
        } catch (SQLException e) {
            c.writeErrMessage(e.getSQLState(), e.getMessage(), e.getErrorCode());
        }
    }

    public static void response(ServerConnection c, String schema, String viewName) {
        try {
            sendOutTheViewInfo(c, schema, viewName);
        } catch (SQLException e) {
            c.writeErrMessage(e.getSQLState(), e.getMessage(), e.getErrorCode());
        }
    }

    public static void sendOutTheViewInfo(ServerConnection c, String schema, String viewName) throws SQLException {
        //check if the view or sharding doesn't exist
        if (schema == null || "".equals(schema)) {
            throw new SQLException("No database selected", "3D000", ErrorCode.ER_NO_DB_ERROR);
        }

        schema = StringUtil.removeBackQuote(schema);
        SchemaMeta schemaMeta = ProxyMeta.getInstance().getTmManager().getCatalogs().get(schema);
        if (schemaMeta == null) {
            throw new SQLException("Table '" + schema + "." + viewName + "' doesn't exist", "42S02", ErrorCode.ER_NO_SUCH_TABLE);
        }
        viewName = StringUtil.removeBackQuote(viewName);
        ViewMeta view = schemaMeta.getViewMetas().get(viewName);
        if (view == null) {
            throw new SQLException("Table '" + schema + "." + viewName + "' doesn't exist", "42S02", ErrorCode.ER_NO_SUCH_TABLE);
        }

        ByteBuffer buffer = c.allocate();
        // write header
        buffer = HEADER.write(buffer, c, true);
        // write fields
        for (FieldPacket field : FIELDS) {
            buffer = field.write(buffer, c, true);
        }
        // write eof
        buffer = EOF.write(buffer, c, true);
        // write rows
        byte packetId = EOF.getPacketId();
        RowDataPacket row = getRow(view, c.getCharset().getResults(), c.getCharset().getCollation());
        row.setPacketId(++packetId);
        buffer = row.write(buffer, c, true);
        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, c, true);
        // write buffer
        c.write(buffer);
    }

    public static RowDataPacket getRow(ViewMeta view, String charset, String collationConnection) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(view.getViewName(), charset));
        if (view.getViewColumnMeta() != null && view.getViewColumnMeta().size() > 0) {
            row.add(StringUtil.encode("create view " + view.getViewName() + view.getViewColumnMetaString() + " as " + view.getSelectSql(), charset));
        } else {
            row.add(StringUtil.encode("create view " + view.getViewName() + " as " + view.getSelectSql(), charset));
        }
        row.add(StringUtil.encode(charset, charset));
        row.add(StringUtil.encode(collationConnection, charset));
        return row;
    }

}
