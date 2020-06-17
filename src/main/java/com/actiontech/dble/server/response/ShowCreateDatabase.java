/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResultSetHeaderPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.route.factory.RouteStrategyFactory;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateDatabaseStatement;

import java.nio.ByteBuffer;

/**
 * Created by collapsar on 2019/7/23.
 */
public final class ShowCreateDatabase {

    private static final int FIELD_COUNT = 2;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("Database", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("Create Database", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i].setPacketId(++packetId);

        EOF.setPacketId(++packetId);
    }

    private ShowCreateDatabase() {
    }

    public static void response(ServerConnection c, String stmt) {
        try {
            stmt = stmt.replaceAll("IF\\s+NOT\\s+EXISTS", "");
            MySqlShowCreateDatabaseStatement statement = (MySqlShowCreateDatabaseStatement) RouteStrategyFactory.getRouteStrategy().parserSQL(stmt);
            String schema = StringUtil.removeBackQuote(statement.getDatabase().toString());
            SchemaConfig sc = DbleServer.getInstance().getConfig().getSchemas().get(schema);
            if (sc == null) {
                throw new Exception("Unknown schema '" + schema + "' in sharding.xml");
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
            RowDataPacket row = getRow(schema, c.getCharset().getResults());
            row.setPacketId(++packetId);
            buffer = row.write(buffer, c, true);
            // write last eof
            EOFPacket lastEof = new EOFPacket();
            lastEof.setPacketId(++packetId);
            buffer = lastEof.write(buffer, c, true);
            // write buffer
            c.write(buffer);
        } catch (Exception e) {
            c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, e.getMessage());
        }
    }

    public static RowDataPacket getRow(String schema, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(schema, charset));
        row.add(StringUtil.encode("CREATE DATABASE `" + schema + "`", charset));
        return row;
    }
}
