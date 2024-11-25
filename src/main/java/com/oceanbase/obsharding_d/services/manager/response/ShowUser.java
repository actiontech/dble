/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.response;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.mysql.PacketUtil;
import com.oceanbase.obsharding_d.config.Fields;
import com.oceanbase.obsharding_d.config.model.user.*;
import com.oceanbase.obsharding_d.net.mysql.*;
import com.oceanbase.obsharding_d.services.manager.ManagerService;
import com.oceanbase.obsharding_d.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.Map;

public final class ShowUser {

    private ShowUser() {
    }

    private static final int FIELD_COUNT = 4;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("Username", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("Manager", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("Readonly", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("Max_con", Fields.FIELD_TYPE_VAR_STRING);
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
            RowDataPacket row = getRow(entry.getValue(), service.getCharset().getResults());
            row.setPacketId(++packetId);
            buffer = row.write(buffer, service, true);
        }

        // write last eof
        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(++packetId);


        lastEof.write(buffer, service);
    }

    private static RowDataPacket getRow(UserConfig user, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        if (user instanceof ShardingUserConfig) {
            ShardingUserConfig shardingUser = (ShardingUserConfig) user;
            if (shardingUser.getTenant() != null) {
                row.add(StringUtil.encode(shardingUser.getName() + ":" + shardingUser.getTenant(), charset));
            } else {
                row.add(StringUtil.encode(shardingUser.getName(), charset));
            }
            row.add(StringUtil.encode("N", charset));
            row.add(StringUtil.encode(shardingUser.isReadOnly() ? "Y" : "N", charset));
            int maxCon = shardingUser.getMaxCon();
            row.add(StringUtil.encode(maxCon == 0 ? "no limit" : maxCon + "", charset));
        } else if (user instanceof ManagerUserConfig) {
            ManagerUserConfig mUser = (ManagerUserConfig) user;
            row.add(StringUtil.encode(user.getName(), charset));
            row.add(StringUtil.encode("Y", charset));
            row.add(StringUtil.encode(mUser.isReadOnly() ? "Y" : "N", charset));
            int maxCon = mUser.getMaxCon();
            row.add(StringUtil.encode(maxCon == 0 ? "no limit" : maxCon + "", charset));
        } else if (user instanceof RwSplitUserConfig) {
            RwSplitUserConfig rUser = (RwSplitUserConfig) user;
            if (rUser.getTenant() != null) {
                row.add(StringUtil.encode(rUser.getName() + ":" + rUser.getTenant(), charset));
            } else {
                row.add(StringUtil.encode(rUser.getName(), charset));
            }
            row.add(StringUtil.encode("N", charset));
            row.add(StringUtil.encode("-", charset));
            int maxCon = rUser.getMaxCon();
            row.add(StringUtil.encode(maxCon == 0 ? "no limit" : maxCon + "", charset));
        } else {
            AnalysisUserConfig aUser = (AnalysisUserConfig) user;
            if (aUser.getTenant() != null) {
                row.add(StringUtil.encode(aUser.getName() + ":" + aUser.getTenant(), charset));
            } else {
                row.add(StringUtil.encode(aUser.getName(), charset));
            }
            row.add(StringUtil.encode("N", charset));
            row.add(StringUtil.encode("-", charset));
            int maxCon = aUser.getMaxCon();
            row.add(StringUtil.encode(maxCon == 0 ? "no limit" : maxCon + "", charset));
        }
        return row;
    }

}
