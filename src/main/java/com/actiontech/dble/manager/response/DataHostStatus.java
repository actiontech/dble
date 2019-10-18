package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.AbstractPhysicalDBPool;
import com.actiontech.dble.backend.datasource.PhysicalDNPoolSingleWH;
import com.actiontech.dble.backend.datasource.PhysicalDatasource;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResultSetHeaderPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.regex.Matcher;

/**
 * Created by szf on 2019/10/28.
 */
public final class DataHostStatus {


    private static final int FIELD_COUNT = 5;
    private static final String STATUS_ALL = "STATUS_ALL";
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("DataHost", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("HostName", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("IP:PORT", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("W/R", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("Status", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        EOF.setPacketId(++packetId);
    }

    private DataHostStatus() {

    }

    public static void execute(Matcher status, ManagerConnection mc) {
        ByteBuffer buffer = mc.allocate();
        // write header
        buffer = HEADER.write(buffer, mc, true);
        // write fields
        for (FieldPacket field : FIELDS) {
            buffer = field.write(buffer, mc, true);
        }
        buffer = EOF.write(buffer, mc, true);
        byte packetId = EOF.getPacketId();
        String stautString = status.group(1);
        String detail = status.group(3);
        if (stautString.trim().equalsIgnoreCase(STATUS_ALL)) {
            //print all dataHost status
            Map<String, AbstractPhysicalDBPool> list = DbleServer.getInstance().getConfig().getDataHosts();

            for (AbstractPhysicalDBPool dh : list.values()) {
                if (dh instanceof PhysicalDNPoolSingleWH) {
                    for (PhysicalDatasource pd : dh.getAllDataSources()) {
                        RowDataPacket row = getRow(pd, (PhysicalDNPoolSingleWH) dh, mc.getCharset().getResults());
                        row.setPacketId(++packetId);
                        buffer = row.write(buffer, mc, true);
                    }
                } else {
                    mc.writeErrMessage(ErrorCode.ER_YES, "dataHost " + dh + " do not exists");
                    return;
                }
            }
        } else {
            String[] nameList = detail.split(",");
            for (String dhName : nameList) {
                AbstractPhysicalDBPool dataHost = DbleServer.getInstance().getConfig().getDataHosts().get(dhName);
                if (dataHost instanceof PhysicalDNPoolSingleWH) {
                    if (dataHost instanceof PhysicalDNPoolSingleWH) {
                        for (PhysicalDatasource pd : dataHost.getAllDataSources()) {
                            RowDataPacket row = getRow(pd, (PhysicalDNPoolSingleWH) dataHost, mc.getCharset().getResults());
                            row.setPacketId(++packetId);
                            buffer = row.write(buffer, mc, true);
                        }
                    } else {
                        mc.writeErrMessage(ErrorCode.ER_YES, "dataHost " + dataHost + " do not exists");
                        return;
                    }
                } else {
                    mc.writeErrMessage(ErrorCode.ER_YES, "dataHost " + dhName + " do not exists");
                    return;
                }
            }
        }

        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, mc, true);
        mc.write(buffer);
    }


    private static RowDataPacket getRow(PhysicalDatasource ph, PhysicalDNPoolSingleWH pool, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(pool.getHostName(), charset));
        row.add(StringUtil.encode(ph.getName(), charset));
        row.add(StringUtil.encode(ph.getConfig().getUrl(), charset));
        row.add(StringUtil.encode(ph.isReadNode() ? "R" : "W", charset));
        row.add(StringUtil.encode(ph.isDisabled() ? "Disabled" : "Enable", charset));
        return row;
    }
}
