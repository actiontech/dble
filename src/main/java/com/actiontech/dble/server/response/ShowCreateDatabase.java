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
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.route.factory.RouteStrategyFactory;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
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

    public static void response(ShardingService shardingService, String stmt) {
        try {
            stmt = stmt.replaceAll("IF\\s+NOT\\s+EXISTS", "");
            MySqlShowCreateDatabaseStatement statement = (MySqlShowCreateDatabaseStatement) RouteStrategyFactory.getRouteStrategy().parserSQL(stmt);
            String schema = StringUtil.removeBackQuote(statement.getDatabase().toString());
            SchemaConfig sc = DbleServer.getInstance().getConfig().getSchemas().get(schema);
            if (sc == null) {
                throw new Exception("Unknown schema '" + schema + "' in config");
            }
            ByteBuffer buffer = shardingService.allocate();
            // writeDirectly header
            buffer = HEADER.write(buffer, shardingService, true);
            // writeDirectly fields
            for (FieldPacket field : FIELDS) {
                buffer = field.write(buffer, shardingService, true);
            }
            // writeDirectly eof
            buffer = EOF.write(buffer, shardingService, true);
            // writeDirectly rows
            byte packetId = EOF.getPacketId();
            RowDataPacket row = getRow(schema, shardingService.getCharset().getResults());
            row.setPacketId(++packetId);
            buffer = row.write(buffer, shardingService, true);
            // writeDirectly last eof
            EOFRowPacket lastEof = new EOFRowPacket();
            lastEof.setPacketId(++packetId);
            lastEof.write(buffer, shardingService);
        } catch (Exception e) {
            shardingService.writeErrMessage(ErrorCode.ER_PARSE_ERROR, e.getMessage());
        }
    }

    public static RowDataPacket getRow(String schema, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(schema, charset));
        row.add(StringUtil.encode("CREATE DATABASE `" + schema + "`", charset));
        return row;
    }
}
