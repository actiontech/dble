package io.mycat.manager.response;

import io.mycat.MycatServer;
import io.mycat.backend.mysql.PacketUtil;
import io.mycat.config.Fields;
import io.mycat.config.model.UserConfig;
import io.mycat.manager.ManagerConnection;
import io.mycat.net.mysql.EOFPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.ResultSetHeaderPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public final class ShowWhiteHost {
    private ShowWhiteHost() {
    }

    private static final int FIELD_COUNT = 2;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("IP", Fields.FIELD_TYPE_VARCHAR);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("USER", Fields.FIELD_TYPE_VARCHAR);
        FIELDS[i++].packetId = ++packetId;


        EOF.packetId = ++packetId;
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
        byte packetId = EOF.packetId;

        Map<String, List<UserConfig>> map = MycatServer.getInstance().getConfig().getFirewall().getWhitehost();
        for (Map.Entry<String, List<UserConfig>> entry : map.entrySet()) {
            List<UserConfig> userConfigs = entry.getValue();
            StringBuilder users = new StringBuilder();
            for (int i = 0; i < userConfigs.size(); i++) {
                if (i > 0) {
                    users.append(",");
                }
                users.append(userConfigs.get(i).getName());
            }
            RowDataPacket row = getRow(entry.getKey(), users.toString(), c.getCharset());
            row.packetId = ++packetId;
            buffer = row.write(buffer, c, true);
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.write(buffer, c, true);

        // write buffer
        c.write(buffer);
    }

    private static RowDataPacket getRow(String ip, String user, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(ip, charset));
        row.add(StringUtil.encode(user, charset));
        return row;
    }


}
