package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.manager.handler.ShowProcesslistHandler;
import com.actiontech.dble.net.ConnectionException;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
        List<RowDataPacket> rows = new ArrayList<>();
        Map<String, Integer> indexs = new HashMap<>();
        Map<String, List<Long>> dataNodeMap = new HashMap<>(4, 1f);
        String charset = c.getCharset().getResults();

        for (NIOProcessor p : DbleServer.getInstance().getFrontProcessors()) {
            for (FrontendConnection fc : p.getFrontends().values()) {
                if (fc == null)
                    break;

                Map<RouteResultsetNode, BackendConnection> backendConns = null;
                if (fc instanceof ServerConnection) {
                    backendConns = ((ServerConnection) fc).getSession2().getTargetMap();
                }
                if (!CollectionUtil.isEmpty(backendConns)) {
                    for (Map.Entry<RouteResultsetNode, BackendConnection> entry : backendConns.entrySet()) {
                        String dataNode = entry.getKey().getName();
                        long threadId = ((MySQLConnection) entry.getValue()).getThreadId();
                        // row data package
                        RowDataPacket row = getRow(fc, dataNode, threadId, charset);
                        rows.add(row);
                        // index
                        indexs.put(dataNode + "." + String.valueOf(threadId), rows.size() - 1);
                        // datanode map
                        if (dataNodeMap.get(dataNode) == null) {
                            List<Long> threadIds = new ArrayList<>(3);
                            threadIds.add(threadId);
                            dataNodeMap.put(dataNode, threadIds);
                        } else {
                            dataNodeMap.get(dataNode).add(threadId);
                        }
                    }
                } else {
                    RowDataPacket row = getRow(fc, null, null, charset);
                    rows.add(row);
                }
            }
        }

        // set 'show processlist' content
        try {
            Map<String, Map<String, String>> backendRes = showProcessList(dataNodeMap);
            for (Map.Entry<String, Integer> entry : indexs.entrySet()) {
                Map<String, String> res = backendRes.get(entry.getKey());
                if (res != null) {
                    int index = entry.getValue();
                    RowDataPacket row = rows.get(index);
                    row.setValue(5, StringUtil.encode(res.get("db"), charset));
                    row.setValue(6, StringUtil.encode(res.get("Command"), charset));
                    row.setValue(7, StringUtil.encode(res.get("Time"), charset));
                    row.setValue(8, StringUtil.encode(res.get("State"), charset));
                    row.setValue(9, StringUtil.encode(res.get("info"), charset));
                }
            }
        } catch (ConnectionException ce) {
            c.writeErrMessage(ErrorCode.ER_BACKEND_CONNECTION, "backend connection acquisition exception!");
            return;
        }

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
        for (RowDataPacket row : rows) {
            row.setPacketId(++packetId);
            buffer = row.write(buffer, c, true);
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, c, true);

        // write buffer
        c.write(buffer);
    }

    private static RowDataPacket getRow(FrontendConnection fc, String dataNode, Long threadId, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        // Front_Id
        row.add(LongUtil.toBytes(fc.getId()));
        // Datanode
        row.add(StringUtil.encode(dataNode == null ? NULL_VAL : dataNode, charset));
        // BconnID
        row.add(threadId == null ? StringUtil.encode(NULL_VAL, charset) : LongUtil.toBytes(threadId));
        // User
        row.add(StringUtil.encode(fc.getUser(), charset));
        // Front_Host
        row.add(StringUtil.encode(fc.getHost() + ":" + fc.getLocalPort(), charset));
        // db
        row.add(StringUtil.encode(NULL_VAL, charset));
        // Command
        row.add(StringUtil.encode(NULL_VAL, charset));
        // Time
        row.add(LongUtil.toBytes(0L));
        // State
        row.add(StringUtil.encode("", charset));
        // Info
        row.add(StringUtil.encode(NULL_VAL, charset));
        return row;
    }

    private static Map<String, Map<String, String>> showProcessList(Map<String, List<Long>> dns) {
        Map<String, Map<String, String>> result = new HashMap<>();
        for (Map.Entry<String, List<Long>> entry : dns.entrySet()) {
            ShowProcesslistHandler handler = new ShowProcesslistHandler(entry.getKey(), entry.getValue());
            handler.execute();
            if (handler.isSuccess()) {
                result.putAll(handler.getResult());
            } else {
                throw new ConnectionException(ErrorCode.ER_BACKEND_CONNECTION, "backend connection acquisition exception!");
            }
        }
        return result;
    }

}
