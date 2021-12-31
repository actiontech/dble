package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.net.IOProcessor;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.connection.FrontendConnection;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.services.FrontendService;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
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

        if (WriteQueueFlowController.isEnableFlowControl()) {
            {
                //find all server connection
                IOProcessor[] processors = DbleServer.getInstance().getFrontProcessors();
                for (IOProcessor p : processors) {
                    for (FrontendConnection fc : p.getFrontends().values()) {
                        if (!fc.isManager() && fc.isFlowControlled()) {
                            RowDataPacket row = new RowDataPacket(FIELD_COUNT);
                            row.add(StringUtil.encode("ServerConnection", service.getCharset().getResults()));
                            row.add(LongUtil.toBytes(fc.getId()));
                            row.add(StringUtil.encode(fc.getHost() + ":" + fc.getLocalPort() + "/" + ((FrontendService) fc.getService()).getSchema() + " user = " + ((FrontendService) fc.getService()).getUser(), service.getCharset().getResults()));
                            row.add(LongUtil.toBytes(fc.getWriteQueue().size()));
                            row.setPacketId(++packetId);
                            buffer = row.write(buffer, service, true);
                        }
                    }
                }
            }
            //find all mysql connection
            {
                IOProcessor[] processors = DbleServer.getInstance().getBackendProcessors();
                for (IOProcessor p : processors) {
                    for (BackendConnection bc : p.getBackends().values()) {
                        MySQLResponseService mc = bc.getBackendService();
                        if (mc.isFlowControlled()) {
                            RowDataPacket row = new RowDataPacket(FIELD_COUNT);
                            row.add(StringUtil.encode("MySQLConnection", service.getCharset().getResults()));
                            row.add(LongUtil.toBytes(mc.getConnection().getThreadId()));
                            row.add(StringUtil.encode(mc.getConnection().getInstance().getConfig().getUrl() + "/" + mc.getSchema() + " id = " + mc.getConnection().getThreadId(), service.getCharset().getResults()));
                            row.add(LongUtil.toBytes(mc.getConnection().getWriteQueue().size()));
                            row.setPacketId(++packetId);
                            buffer = row.write(buffer, service, true);
                        }
                    }
                }
            }
        }

        // write last eof
        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(++packetId);


        lastEof.write(buffer, service);
    }
}
