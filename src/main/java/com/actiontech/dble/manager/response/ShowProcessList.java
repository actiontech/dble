package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.manager.handler.ShowProcesslistHandler;
import com.actiontech.dble.net.FrontendConnection;
import com.actiontech.dble.net.NIOProcessor;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResultSetHeaderPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.util.CollectionUtil;
import com.actiontech.dble.util.LongUtil;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * ShowProcessList
 *
 * @author collapsar
 * @author collapsar
 */
public final class ShowProcessList {
    private ShowProcessList() {
    }

    private static final int FIELD_COUNT = 10;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();
    private static final String NULL_VAL = "NULL";

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("Front_Id", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("Datanode", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("BconnID", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("User", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("Front_Host", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("db", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("Command", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("Time", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("State", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("Info", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i].setPacketId(++packetId);

        EOF.setPacketId(++packetId);
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

        NIOProcessor[] processors = DbleServer.getInstance().getFrontProcessors();
        for (NIOProcessor p : processors) {
            for (FrontendConnection fc : p.getFrontends().values()) {
                if (fc == null) {
                    break;
                }
                Map<RouteResultsetNode, BackendConnection> backendConns = null;
                if (fc instanceof ServerConnection) {
                    backendConns = ((ServerConnection) fc).getSession2().getTargetMap();
                }
                if (!CollectionUtil.isEmpty(backendConns)) {
                    for (Map.Entry<RouteResultsetNode, BackendConnection> entry : backendConns.entrySet()) {
                        RowDataPacket row = getRow(fc, entry.getKey().getName(), entry.getValue(), c.getCharset().getResults());
                        row.setPacketId(++packetId);
                        buffer = row.write(buffer, c, true);
                    }
                } else {
                    RowDataPacket row = getRow(fc, null, null, c.getCharset().getResults());
                    row.setPacketId(++packetId);
                    buffer = row.write(buffer, c, true);
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

    private static RowDataPacket getRow(FrontendConnection fc, String dataNode, BackendConnection conn, String charset) {
        MySQLConnection mysqlConn = null;
        if (conn instanceof MySQLConnection) {
            mysqlConn = (MySQLConnection) conn;
        }
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        // Front_Id
        row.add(LongUtil.toBytes(fc.getId()));
        // Datanode
        row.add(StringUtil.encode(dataNode == null ? NULL_VAL : dataNode, charset));
        // BconnID
        row.add(mysqlConn == null ? StringUtil.encode(NULL_VAL, charset) : LongUtil.toBytes(mysqlConn.getThreadId()));
        // User
        row.add(StringUtil.encode(fc.getUser(), charset));
        // Front_Host
        row.add(StringUtil.encode(fc.getHost() + ":" + fc.getLocalPort(), charset));
        // db
        row.add(StringUtil.encode(mysqlConn == null ? NULL_VAL : conn.getSchema(), charset));
        // show prcesslist
        String command = NULL_VAL;
        String time = "0";
        String state = "";
        String info = NULL_VAL;
        if (mysqlConn != null) {
            ShowProcesslistHandler handler = new ShowProcesslistHandler(dataNode, mysqlConn.getThreadId());
            handler.execute();
            Map<String, String> result = handler.getResult();
            if (!CollectionUtil.isEmpty(result)) {
                command = result.get("Command");
                time = result.get("Time");
                state = result.get("State");
                info = result.get("Info");
            }
        }

        // Command
        row.add(StringUtil.encode(command, charset));
        // Time
        row.add(LongUtil.toBytes(Long.parseLong(time)));
        // State
        row.add(StringUtil.encode(state, charset));
        // Info
        row.add(StringUtil.encode(info, charset));
        return row;
    }


}
