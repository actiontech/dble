/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.handler;

import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.util.SchemaUtil;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.ast.statement.SQLUnionQuery;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;

import java.nio.ByteBuffer;
import java.util.List;

public final class MysqlSystemSchemaHandler {
    private MysqlSystemSchemaHandler() {
    }

    public static final String SCHEMATA_TABLE = "SCHEMATA";
    public static final String INFORMATION_SCHEMA = "INFORMATION_SCHEMA";

    public static void handle(ServerConnection sc, SchemaUtil.SchemaInfo schemaInfo, SQLSelectQuery sqlSelectQuery) {
        MySqlSelectQueryBlock mySqlSelectQueryBlock = null;
        if (sqlSelectQuery instanceof MySqlSelectQueryBlock) {
            mySqlSelectQueryBlock = (MySqlSelectQueryBlock) sqlSelectQuery;
        } else if (sqlSelectQuery instanceof SQLUnionQuery) {
            SQLUnionQuery mySqlSelectUnionQuery = (SQLUnionQuery) sqlSelectQuery;
            mySqlSelectQueryBlock = (MySqlSelectQueryBlock) mySqlSelectUnionQuery.getFirstQueryBlock();
        }

        if (mySqlSelectQueryBlock == null) {
            sc.write(sc.writeToBuffer(OkPacket.OK, sc.allocate()));
            return;
        }

        if (schemaInfo != null && INFORMATION_SCHEMA.equals(schemaInfo.getSchema().toUpperCase()) &&
                SCHEMATA_TABLE.equals(schemaInfo.getTable().toUpperCase())) {
            MysqlInformationSchemaHandler.handle(schemaInfo, sc);
            return;
        }

        FieldPacket[] fields = generateFieldPacket(mySqlSelectQueryBlock.getSelectList());
        doWrite(fields.length, fields, null, sc);
    }

    private static FieldPacket[] generateFieldPacket(List<SQLSelectItem> selectList) {
        FieldPacket[] fields = new FieldPacket[selectList.size()];
        for (int i = 0; i < selectList.size(); i++) {
            String columnName;
            SQLSelectItem selectItem = selectList.get(i);
            if (selectItem.getAlias() != null) {
                columnName = selectList.get(i).getAlias();
            } else {
                columnName = selectItem.toString();
            }
            fields[i] = PacketUtil.getField(columnName, Fields.FIELD_TYPE_VAR_STRING);
        }
        return fields;
    }

    /**
     * @param fieldCount
     * @param fields
     * @param c
     */
    public static void doWrite(int fieldCount, FieldPacket[] fields, RowDataPacket[] rows, ServerConnection c) {

        ByteBuffer buffer = c.allocate();

        // write header
        ResultSetHeaderPacket header = PacketUtil.getHeader(fieldCount);
        byte packetId = header.getPacketId();
        buffer = header.write(buffer, c, true);

        // write fields
        for (FieldPacket field : fields) {
            field.setPacketId(++packetId);
            buffer = field.write(buffer, c, true);
        }

        // write eof
        EOFPacket eof = new EOFPacket();
        eof.setPacketId(++packetId);
        buffer = eof.write(buffer, c, true);

        // write rows
        if (rows != null) {
            for (RowDataPacket row : rows) {
                row.setPacketId(++packetId);
                buffer = row.write(buffer, c, true);
            }
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, c, true);

        // post write
        c.write(buffer);
    }
}

