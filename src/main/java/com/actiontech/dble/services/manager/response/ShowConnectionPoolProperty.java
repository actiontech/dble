/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.model.db.PoolConfig;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.services.manager.ManagerService;
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

        FIELDS[i] = PacketUtil.getField("DB_INSTANCE", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("PROPERTY", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("VALUE", Fields.FIELD_TYPE_LONG);
        FIELDS[i].setPacketId(++packetId);

        EOF.setPacketId(++packetId);
    }

    private ShowConnectionPoolProperty() {
    }

    public static void execute(ManagerService service) {
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

        PoolConfig poolConfig;
        for (PhysicalDbGroup group : DbleServer.getInstance().getConfig().getDbGroups().values()) {
            for (PhysicalDbInstance instance : group.getDbInstances(true)) {
                poolConfig = instance.getConfig().getPoolConfig();
                RowDataPacket row = getRow(group.getGroupName(), instance.getName(), "minCon", instance.getConfig().getMinCon() + "", service.getCharset().getClient());
                row.setPacketId(++packetId);
                buffer = row.write(buffer, service, true);
                row = getRow(group.getGroupName(), instance.getName(), "maxCon", instance.getConfig().getMaxCon() + "", service.getCharset().getClient());
                row.setPacketId(++packetId);
                buffer = row.write(buffer, service, true);
                row = getRow(group.getGroupName(), instance.getName(), "testOnCreate", poolConfig.getTestOnCreate() + "", service.getCharset().getClient());
                row.setPacketId(++packetId);
                buffer = row.write(buffer, service, true);
                row = getRow(group.getGroupName(), instance.getName(), "testOnBorrow", poolConfig.getTestOnBorrow() + "", service.getCharset().getClient());
                row.setPacketId(++packetId);
                buffer = row.write(buffer, service, true);
                row = getRow(group.getGroupName(), instance.getName(), "testOnReturn", poolConfig.getTestOnReturn() + "", service.getCharset().getClient());
                row.setPacketId(++packetId);
                buffer = row.write(buffer, service, true);
                row = getRow(group.getGroupName(), instance.getName(), "testWhileIdle", poolConfig.getTestWhileIdle() + "", service.getCharset().getClient());
                row.setPacketId(++packetId);
                buffer = row.write(buffer, service, true);
                row = getRow(group.getGroupName(), instance.getName(), "connectionHeartbeatTimeout", poolConfig.getConnectionHeartbeatTimeout() + "", service.getCharset().getClient());
                row.setPacketId(++packetId);
                buffer = row.write(buffer, service, true);
                row = getRow(group.getGroupName(), instance.getName(), "connectionTimeout", poolConfig.getConnectionTimeout() + "", service.getCharset().getClient());
                row.setPacketId(++packetId);
                buffer = row.write(buffer, service, true);
                row = getRow(group.getGroupName(), instance.getName(), "heartbeatPeriodMillis", poolConfig.getHeartbeatPeriodMillis() + "", service.getCharset().getClient());
                row.setPacketId(++packetId);
                buffer = row.write(buffer, service, true);
                row = getRow(group.getGroupName(), instance.getName(), "idleTimeout", poolConfig.getIdleTimeout() + "", service.getCharset().getClient());
                row.setPacketId(++packetId);
                buffer = row.write(buffer, service, true);
                row = getRow(group.getGroupName(), instance.getName(), "evictorShutdownTimeoutMillis", poolConfig.getEvictorShutdownTimeoutMillis() + "", service.getCharset().getClient());
                row.setPacketId(++packetId);
                buffer = row.write(buffer, service, true);
                row = getRow(group.getGroupName(), instance.getName(), "timeBetweenEvictionRunsMillis", poolConfig.getTimeBetweenEvictionRunsMillis() + "", service.getCharset().getClient());
                row.setPacketId(++packetId);
                buffer = row.write(buffer, service, true);
            }
        }

        // write last eof
        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(++packetId);

        // write buffer
        lastEof.write(buffer, service);
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
