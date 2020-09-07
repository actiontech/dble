/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.route.factory.RouteStrategyFactory;
import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.services.manager.information.ManagerBaseTable;
import com.actiontech.dble.services.manager.information.ManagerSchemaInfo;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlExplainStatement;

import java.nio.ByteBuffer;
import java.sql.SQLException;

/**
 * ShowDatabase
 *
 * @author mycat
 * @author mycat
 */
public final class Describe {
    private Describe() {
    }

    private static final int FIELD_COUNT = 6;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("Field", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("Type", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("Null", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("Key", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("Default", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("Extra", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i].setPacketId(++packetId);

        EOF.setPacketId(++packetId);
    }

    public static void execute(String stmt, ManagerService service) {
        String schemaName;
        String tableName;
        try {
            SQLStatement statement = RouteStrategyFactory.getRouteStrategy().parserSQL(stmt);
            MySqlExplainStatement describeStatement = (MySqlExplainStatement) statement;
            SchemaUtil.SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(service.getUser(), service.getSchema(), describeStatement.getTableName(), null);
            schemaName = schemaInfo.getSchema().toLowerCase();
            tableName = schemaInfo.getTable().toLowerCase();
        } catch (SQLException e) {
            service.writeErrMessage(e.getSQLState(), e.getMessage(), e.getErrorCode());
            return;
        }
        if (!ManagerSchemaInfo.SCHEMA_NAME.equals(schemaName)) {
            service.writeErrMessage("42000", "Unknown database '" + schemaName + "'", ErrorCode.ER_BAD_DB_ERROR);
            return;
        }
        ManagerBaseTable table = ManagerSchemaInfo.getInstance().getTables().get(tableName);
        if (table == null) {
            service.writeErrMessage("42S02", " Table '" + ManagerSchemaInfo.SCHEMA_NAME + "." + tableName + "' doesn't exist", ErrorCode.ER_NO_SUCH_TABLE);
            return;
        }
        ByteBuffer buffer = service.allocate();

        // write header
        buffer = HEADER.write(buffer, service, true);

        // write fields
        for (FieldPacket field : FIELDS) {
            buffer = field.write(buffer, service, true);
        }

        // write eof
        buffer = EOF.write(buffer, service, true);

        // write rows
        byte packetId = EOF.getPacketId();
        for (ColumnMeta column : table.getColumnsMeta()) {
            RowDataPacket row = new RowDataPacket(FIELD_COUNT);
            row.add(StringUtil.encode(column.getName(), service.getCharset().getResults()));
            row.add(StringUtil.encode(column.getDataType(), service.getCharset().getResults()));
            row.add(StringUtil.encode(column.isCanNull() ? "YES" : "NO", service.getCharset().getResults()));
            row.add(StringUtil.encode(column.isPrimaryKey() ? "PRI" : "", service.getCharset().getResults()));
            row.add(StringUtil.encode(column.getDefaultVal(), service.getCharset().getResults()));
            row.add(StringUtil.encode("", service.getCharset().getResults()));
            row.setPacketId(++packetId);
            buffer = row.write(buffer, service, true);
        }


        // write lastEof
        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(++packetId);
        // write buffer
        lastEof.write(buffer, service);
    }

}
