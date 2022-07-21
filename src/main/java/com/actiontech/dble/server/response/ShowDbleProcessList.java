/*
 * Copyright (C) 2016-2022 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.net.ConnectionException;
import com.actiontech.dble.net.IOProcessor;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.connection.FrontendConnection;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.services.manager.handler.ShowProcesslistHandler;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.services.rwsplit.RWSplitService;
import com.actiontech.dble.util.CollectionUtil;
import com.actiontech.dble.util.LongUtil;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * ShowProcessList
 *
 * @author collapsar
 */
public final class ShowDbleProcessList {
    private ShowDbleProcessList() {
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

        FIELDS[i] = PacketUtil.getField("db_instance", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("MysqlId", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("User", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("Front_Host", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("db", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("Command", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("Time", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("State", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("Info", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i].setPacketId(++packetId);

        EOF.setPacketId(++packetId);
    }

    public static void response(ShardingService service) {
        List<RowDataPacket> rows = new ArrayList<>();
        Map<String, Integer> indexs = new HashMap<>();
        Map<PhysicalDbInstance, List<Long>> dbInstanceMap = new HashMap<>(8);
        String charset = service.getCharset().getResults();

        for (IOProcessor p : DbleServer.getInstance().getFrontProcessors()) {
            for (FrontendConnection fc : p.getFrontends().values()) {
                if (fc == null || !fc.isAuthorized() || fc.isManager()) {
                    continue;
                }
                /*
                 only show your own task
                 */
                if (!Objects.equals(fc.getFrontEndService().getUser(), service.getUser())) {
                    continue;
                }

                if (fc.getService() instanceof ShardingService) {
                    Map<RouteResultsetNode, BackendConnection> backendConns = ((ShardingService) fc.getService()).getSession2().getTargetMap();
                    if (!CollectionUtil.isEmpty(backendConns)) {
                        for (Map.Entry<RouteResultsetNode, BackendConnection> entry : backendConns.entrySet()) {
                            addRow(fc, entry.getValue(), charset, rows, indexs, dbInstanceMap);
                        }
                    } else {
                        rows.add(getRow(fc, null, null, charset));
                    }
                } else {
                    BackendConnection conn = ((RWSplitService) fc.getService()).getSession2().getConn();
                    if (conn != null) {
                        addRow(fc, conn, charset, rows, indexs, dbInstanceMap);
                    } else {
                        rows.add(getRow(fc, null, null, charset));
                    }
                }
            }
        }

        // set 'show processlist' content
        try {
            Map<String, Map<String, String>> backendRes = showProcessList(dbInstanceMap);
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
            service.writeErrMessage(ErrorCode.ER_BACKEND_CONNECTION, "backend connection acquisition exception!");
            return;
        }

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
        for (RowDataPacket row : rows) {
            row.setPacketId(++packetId);
            buffer = row.write(buffer, service, true);
        }

        // write last eof
        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(++packetId);


        lastEof.write(buffer, service);
    }

    private static void addRow(FrontendConnection fconn, BackendConnection bconn, String charset, List<RowDataPacket> rows,
                               Map<String, Integer> indexs, Map<PhysicalDbInstance, List<Long>> dbInstanceMap) {
        long threadId = bconn.getThreadId();
        PhysicalDbInstance dbInstance = (PhysicalDbInstance) bconn.getInstance();
        rows.add(getRow(fconn, dbInstance.getName(), threadId, charset));
        // index
        indexs.put(dbInstance.getName() + "." + threadId, rows.size() - 1);
        // dbInstance map
        if (dbInstanceMap.get(dbInstance) == null) {
            List<Long> threadIds = new ArrayList<>(10);
            threadIds.add(threadId);
            dbInstanceMap.put(dbInstance, threadIds);
        } else {
            dbInstanceMap.get(dbInstance).add(threadId);
        }
    }

    private static RowDataPacket getRow(FrontendConnection fc, String dbInstance, Long threadId, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        // Front_Id
        row.add(LongUtil.toBytes(fc.getId()));
        // dbInstance
        row.add(StringUtil.encode(dbInstance == null ? NULL_VAL : dbInstance, charset));
        // BconnID
        row.add(threadId == null ? StringUtil.encode(NULL_VAL, charset) : LongUtil.toBytes(threadId));
        // User
        row.add(StringUtil.encode(fc.getFrontEndService().getUser().getFullName(), charset));
        // Front_Host
        row.add(StringUtil.encode(fc.getHost() + ":" + fc.getLocalPort(), charset));
        // db
        row.add(StringUtil.encode(NULL_VAL, charset));
        // Command
        row.add(StringUtil.encode(NULL_VAL, charset));
        // Time
        row.add(StringUtil.encode("0", charset));
        // State
        row.add(StringUtil.encode("", charset));
        // Info
        row.add(StringUtil.encode(NULL_VAL, charset));
        return row;
    }

    private static Map<String, Map<String, String>> showProcessList(Map<PhysicalDbInstance, List<Long>> dns) {
        Map<String, Map<String, String>> result = new HashMap<>();
        for (Map.Entry<PhysicalDbInstance, List<Long>> entry : dns.entrySet()) {
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
