/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.server.response;

import com.oceanbase.obsharding_d.backend.mysql.PacketUtil;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.config.Fields;
import com.oceanbase.obsharding_d.meta.SchemaMeta;
import com.oceanbase.obsharding_d.meta.ViewMeta;
import com.oceanbase.obsharding_d.net.mysql.*;
import com.oceanbase.obsharding_d.route.parser.util.DruidUtil;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;
import com.oceanbase.obsharding_d.singleton.ProxyMeta;
import com.oceanbase.obsharding_d.util.StringUtil;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLShowCreateViewStatement;

import java.nio.ByteBuffer;
import java.sql.SQLSyntaxErrorException;

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

    public static void response(ShardingService service, String stmt) {
        try {
            SQLShowCreateViewStatement statement = (SQLShowCreateViewStatement) DruidUtil.parseMultiSQL(stmt);
            String schema = null;
            String view = null;
            if (statement.getName() instanceof SQLPropertyExpr) {
                //show create view with sharding
                SQLPropertyExpr sqlPropertyExpr = (SQLPropertyExpr) statement.getName();
                //protocol not equals the nomul things
                schema = sqlPropertyExpr.getOwner().toString();
                view = sqlPropertyExpr.getName();
            } else if (statement.getName() instanceof SQLIdentifierExpr) {
                schema = service.getSchema();
                view = statement.getName().toString();
            }
            sendOutTheViewInfo(service, schema, view);
        } catch (SQLSyntaxErrorException syntaxExe) {
            service.writeErrMessage("42000", syntaxExe.getMessage(), ErrorCode.ER_PARSE_ERROR);
        }
    }

    public static void response(ShardingService service, String schema, String viewName) {
        sendOutTheViewInfo(service, schema, viewName);
    }

    public static void sendOutTheViewInfo(ShardingService service, String schema, String viewName) {
        //check if the view or sharding doesn't exist
        if (schema == null || "".equals(schema)) {
            service.writeErrMessage("3D000", "No database selected", ErrorCode.ER_NO_DB_ERROR);
            return;
        }

        schema = StringUtil.removeBackQuote(schema);
        SchemaMeta schemaMeta = ProxyMeta.getInstance().getTmManager().getCatalogs().get(schema);
        if (schemaMeta == null) {
            service.writeErrMessage("42S02", "Table '" + schema + "." + viewName + "' doesn't exist", ErrorCode.ER_NO_SUCH_TABLE);
            return;
        }
        viewName = StringUtil.removeBackQuote(viewName);
        ViewMeta view = schemaMeta.getViewMetas().get(viewName);
        if (view == null) {
            service.writeErrMessage("42S02", "Table '" + schema + "." + viewName + "' doesn't exist", ErrorCode.ER_NO_SUCH_TABLE);
            return;
        }

        ByteBuffer buffer = service.allocate();
        // writeDirectly header
        buffer = HEADER.write(buffer, service, true);
        // writeDirectly fields
        for (FieldPacket field : FIELDS) {
            buffer = field.write(buffer, service, true);
        }
        // writeDirectly eof
        buffer = EOF.write(buffer, service, true);
        // writeDirectly rows
        byte packetId = EOF.getPacketId();
        RowDataPacket row = getRow(view, service.getCharset().getResults(), service.getCharset().getCollation());
        row.setPacketId(++packetId);
        buffer = row.write(buffer, service, true);
        // writeDirectly last eof
        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(++packetId);
        lastEof.write(buffer, service);
    }

    public static RowDataPacket getRow(ViewMeta view, String charset, String collationConnection) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(view.getViewName(), charset));
        if (view.getViewColumnMeta() != null && view.getViewColumnMeta().size() > 0) {
            row.add(StringUtil.encode("create view " + view.getViewName() + view.getViewColumnMetaString() + " as" + view.getSelectSql(), charset));
        } else {
            row.add(StringUtil.encode("create view " + view.getViewName() + " as" + view.getSelectSql(), charset));
        }
        row.add(StringUtil.encode(charset, charset));
        row.add(StringUtil.encode(collationConnection, charset));
        return row;
    }

}
