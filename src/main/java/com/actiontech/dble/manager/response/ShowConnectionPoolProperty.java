/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.backend.pool.PoolConfig;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResultSetHeaderPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;

/**
 * Show Active Connection
 *
 * @author mycat
 */
public final class ShowConnectionPoolProperty {
    private static final int FIELD_COUNT = 4;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("DB_GROUP", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("DB_INSTANCE", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("PROPERTY", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("VALUE", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i].setPacketId(++packetId);

        EOF.setPacketId(++packetId);
    }

    private ShowConnectionPoolProperty() {
    }

    public static void execute(ManagerConnection c) {
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

        PoolConfig poolConfig;
        for (PhysicalDbGroup group : DbleServer.getInstance().getConfig().getDbGroups().values()) {
            for (PhysicalDbInstance instance : group.getAllDbInstances()) {
                poolConfig = instance.getConfig().getPoolConfig();
                RowDataPacket row = getRow(group.getGroupName(), instance.getName(), "minCon", instance.getConfig().getMinCon() + "", c.getCharset().getClient());
                row.setPacketId(++packetId);
                buffer = row.write(buffer, c, true);
                row = getRow(group.getGroupName(), instance.getName(), "maxCon", instance.getConfig().getMaxCon() + "", c.getCharset().getClient());
                row.setPacketId(++packetId);
                buffer = row.write(buffer, c, true);
                row = getRow(group.getGroupName(), instance.getName(), "testOnCreate", poolConfig.getTestOnCreate() + "", c.getCharset().getClient());
                row.setPacketId(++packetId);
                buffer = row.write(buffer, c, true);
                row = getRow(group.getGroupName(), instance.getName(), "testOnBorrow", poolConfig.getTestOnBorrow() + "", c.getCharset().getClient());
                row.setPacketId(++packetId);
                buffer = row.write(buffer, c, true);
                row = getRow(group.getGroupName(), instance.getName(), "testOnReturn", poolConfig.getTestOnReturn() + "", c.getCharset().getClient());
                row.setPacketId(++packetId);
                buffer = row.write(buffer, c, true);
                row = getRow(group.getGroupName(), instance.getName(), "testWhileIdle", poolConfig.getTestWhileIdle() + "", c.getCharset().getClient());
                row.setPacketId(++packetId);
                buffer = row.write(buffer, c, true);
                row = getRow(group.getGroupName(), instance.getName(), "connectionHeartbeatTimeout", poolConfig.getConnectionHeartbeatTimeout() + "", c.getCharset().getClient());
                row.setPacketId(++packetId);
                buffer = row.write(buffer, c, true);
                row = getRow(group.getGroupName(), instance.getName(), "connectionTimeout", poolConfig.getConnectionTimeout() + "", c.getCharset().getClient());
                row.setPacketId(++packetId);
                buffer = row.write(buffer, c, true);
                row = getRow(group.getGroupName(), instance.getName(), "heartbeatPeriodMillis", poolConfig.getHeartbeatPeriodMillis() + "", c.getCharset().getClient());
                row.setPacketId(++packetId);
                buffer = row.write(buffer, c, true);
                row = getRow(group.getGroupName(), instance.getName(), "idleTimeout", poolConfig.getIdleTimeout() + "", c.getCharset().getClient());
                row.setPacketId(++packetId);
                buffer = row.write(buffer, c, true);
                row = getRow(group.getGroupName(), instance.getName(), "evictorShutdownTimeoutMillis", poolConfig.getEvictorShutdownTimeoutMillis() + "", c.getCharset().getClient());
                row.setPacketId(++packetId);
                buffer = row.write(buffer, c, true);
                row = getRow(group.getGroupName(), instance.getName(), "timeBetweenEvictionRunsMillis", poolConfig.getTimeBetweenEvictionRunsMillis() + "", c.getCharset().getClient());
                row.setPacketId(++packetId);
                buffer = row.write(buffer, c, true);
            }
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, c, true);

        // write buffer
        c.write(buffer);
    }

    private static RowDataPacket getRow(String dbGroup, String dbInstance, String property, String value, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(dbGroup, charset));
        row.add(StringUtil.encode(dbInstance, charset));
        row.add(StringUtil.encode(property, charset));
        row.add(StringUtil.encode(value, charset));
        return row;
    }

}
