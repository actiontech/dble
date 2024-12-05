/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.response;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.mysql.PacketUtil;
import com.oceanbase.obsharding_d.config.Fields;
import com.oceanbase.obsharding_d.config.model.user.ShardingUserConfig;
import com.oceanbase.obsharding_d.config.model.user.UserConfig;
import com.oceanbase.obsharding_d.config.model.user.UserName;
import com.oceanbase.obsharding_d.config.model.user.UserPrivilegesConfig;
import com.oceanbase.obsharding_d.net.mysql.*;
import com.oceanbase.obsharding_d.services.manager.ManagerService;
import com.oceanbase.obsharding_d.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;

public final class ShowUserPrivilege {

    private ShowUserPrivilege() {
    }

    private static final int FIELD_COUNT = 7;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();
    private static final int[] ALL_PRIVILEGES = new int[]{1, 1, 1, 1};

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("Username", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("Schema", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("Table", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("INSERT", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("UPDATE", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("SELECT", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("DELETE", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i].setPacketId(++packetId);

        EOF.setPacketId(++packetId);
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
        Map<UserName, UserConfig> users = OBsharding_DServer.getInstance().getConfig().getUsers();
        for (Map.Entry<UserName, UserConfig> entry : users.entrySet()) {
            UserName userName = entry.getKey();
            UserConfig user = entry.getValue();
            // skip manager
            if (!(user instanceof ShardingUserConfig)) {
                continue;
            }
            ShardingUserConfig sUser = (ShardingUserConfig) user;

            String tableName;
            // privileges
            int[] pri;
            // user privilege config
            UserPrivilegesConfig userPrivilegesConfig = sUser.getPrivilegesConfig();
            boolean noNeedCheck = userPrivilegesConfig == null || !userPrivilegesConfig.isCheck();
            for (String schema : sUser.getSchemas()) {
                if (noNeedCheck || userPrivilegesConfig.getSchemaPrivilege(schema) == null) {
                    tableName = "*";
                    pri = ALL_PRIVILEGES;
                    RowDataPacket row = getRow(userName.getFullName(), schema, tableName, pri, service.getCharset().getResults());
                    row.setPacketId(++packetId);
                    buffer = row.write(buffer, service, true);
                } else {
                    UserPrivilegesConfig.SchemaPrivilege schemaPrivilege = userPrivilegesConfig.getSchemaPrivilege(schema);
                    Set<String> tables = schemaPrivilege.getTables();
                    for (String tn : tables) {
                        tableName = tn;
                        pri = schemaPrivilege.getTablePrivilege(tn).getDml();
                        RowDataPacket row = getRow(userName.getFullName(), schema, tableName, pri, service.getCharset().getResults());
                        row.setPacketId(++packetId);
                        buffer = row.write(buffer, service, true);
                    }
                    tableName = "*";
                    pri = schemaPrivilege.getDml();
                    RowDataPacket row = getRow(userName.getFullName(), schema, tableName, pri, service.getCharset().getResults());
                    row.setPacketId(++packetId);
                    buffer = row.write(buffer, service, true);
                }
            }
        }

        // write last eof
        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(++packetId);


        lastEof.write(buffer, service);
    }

    private static RowDataPacket getRow(String userName, String schema, String table, int[] pri, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(userName, charset));
        row.add(StringUtil.encode(schema, charset));
        row.add(StringUtil.encode(table, charset));
        for (int p : pri) {
            row.add(StringUtil.encode(p == 1 ? "Y" : "N", charset));
        }
        return row;
    }

}
