package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.FrontendConnection;
import com.actiontech.dble.net.NIOProcessor;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResultSetHeaderPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.singleton.WriteQueueFlowController;
import com.actiontech.dble.util.LongUtil;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;

/**
 * Created by szf on 2020/4/10.
 */
public final class FlowControlList {

    private static final int FIELD_COUNT = 4;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("CONNECTION_TYPE", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("CONNECTION_ID", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("CONNECTION_INFO", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("WRITE_QUEUE_SIZE", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i].setPacketId(++packetId);

        EOF.setPacketId(++packetId);
    }

    private FlowControlList() {

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

        if (WriteQueueFlowController.isEnableFlowControl()) {
            {
                //find all server connection
                NIOProcessor[] processors = DbleServer.getInstance().getFrontProcessors();
                for (NIOProcessor p : processors) {
                    for (FrontendConnection fc : p.getFrontends().values()) {
                        if (fc instanceof ServerConnection && fc.isFlowControlled()) {
                            RowDataPacket row = new RowDataPacket(FIELD_COUNT);
                            row.add(StringUtil.encode("ServerConnection", c.getCharset().getResults()));
                            row.add(LongUtil.toBytes(fc.getId()));
                            row.add(StringUtil.encode(fc.getHost() + ":" + fc.getLocalPort() + "/" + fc.getSchema() + " user = " + fc.getUser(), c.getCharset().getResults()));
                            row.add(LongUtil.toBytes(fc.getWriteQueue().size()));
                            row.setPacketId(++packetId);
                            buffer = row.write(buffer, c, true);
                        }
                    }
                }
            }
            //find all mysql connection
            {
                NIOProcessor[] processors = DbleServer.getInstance().getBackendProcessors();
                for (NIOProcessor p : processors) {
                    for (BackendConnection bc : p.getBackends().values()) {
                        MySQLConnection mc = (MySQLConnection) bc;
                        if (mc.isFlowControlled()) {
                            RowDataPacket row = new RowDataPacket(FIELD_COUNT);
                            row.add(StringUtil.encode("MySQLConnection", c.getCharset().getResults()));
                            row.add(LongUtil.toBytes(mc.getThreadId()));
                            row.add(StringUtil.encode(mc.getDbInstance().getConfig().getUrl() + "/" + mc.getSchema() + " id = " + mc.getThreadId(), c.getCharset().getResults()));
                            row.add(LongUtil.toBytes(mc.getWriteQueue().size()));
                            row.setPacketId(++packetId);
                            buffer = row.write(buffer, c, true);
                        }
                    }
                }
            }
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, c, true);

        // write buffer
        c.write(buffer);
    }

}
