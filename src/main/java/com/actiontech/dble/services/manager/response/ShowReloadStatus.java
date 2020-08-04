package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.cluster.ClusterGeneralConfig;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.meta.ReloadManager;
import com.actiontech.dble.meta.ReloadStatus;
import com.actiontech.dble.util.FormatUtil;
import com.actiontech.dble.util.LongUtil;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;

import static com.actiontech.dble.meta.ReloadStatus.*;

/**
 * Created by szf on 2019/7/15.
 */
public final class ShowReloadStatus {

    private ShowReloadStatus() {

    }

    private static final int FIELD_COUNT = 8;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("INDEX", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("CLUSTER", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("RELOAD_TYPE",
                Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("RELOAD_STATUS",
                Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("LAST_RELOAD_START",
                Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("LAST_RELOAD_END",
                Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("TRIGGER_TYPE",
                Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("END_TYPE",
                Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
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
        RowDataPacket row = getRow(service.getCharset().getResults());
        row.setPacketId(++packetId);
        buffer = row.write(buffer, service, true);

        // write last eof
        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(++packetId);


        lastEof.write(buffer, service);
    }

    private static RowDataPacket getRow(String charset) {
        ReloadStatus status = ReloadManager.getReloadInstance().getStatus();
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        if (status == null) {
            row.add(LongUtil.toBytes(0));
            row.add(StringUtil.encode(ClusterGeneralConfig.getInstance().getClusterType(), charset));
            row.add(StringUtil.encode("", charset));
            row.add(StringUtil.encode(RELOAD_STATUS_NONE, charset));
            row.add(StringUtil.encode("", charset));
            row.add(StringUtil.encode("", charset));
            row.add(StringUtil.encode("", charset));
            row.add(StringUtil.encode("", charset));
        } else {
            row.add(LongUtil.toBytes(status.getId()));
            row.add(StringUtil.encode(status.getClusterType(), charset));
            row.add(StringUtil.encode(status.getReloadType().toString(), charset));
            row.add(StringUtil.encode(status.getStatus(), charset));
            row.add(StringUtil.encode(FormatUtil.formatDate(status.getLastReloadStart()), charset));
            row.add(StringUtil.encode(FormatUtil.formatDate(status.getLastReloadEnd()), charset));
            row.add(StringUtil.encode(status.getTriggerType(), charset));
            row.add(StringUtil.encode(status.getLastReloadEnd() != 0 ? (status.isReloadInterrupted() ? RELOAD_INTERRUPUTED : RELOAD_END_NORMAL) : "", charset));
        }
        return row;
    }
}
