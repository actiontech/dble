/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.heartbeat.HeartbeatSQLJob;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.net.IOProcessor;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.connection.PooledConnection;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.net.service.AuthService;
import com.actiontech.dble.route.factory.RouteStrategyFactory;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.util.*;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;

import java.nio.ByteBuffer;
import java.sql.SQLSyntaxErrorException;
import java.util.HashMap;
import java.util.Map;

/**
 * ShowBackend
 *
 * @author mycat
 */
public final class ShowBackend {
    private static final int FIELD_COUNT = 23;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("processor", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("BACKEND_ID", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("MYSQLID", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("HOST", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("PORT", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("LOCAL_TCP_PORT", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("NET_IN", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("NET_OUT", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("ACTIVE_TIME(S)", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("CLOSED", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        // fields[i] = PacketUtil.getField("run", Fields.FIELD_TYPE_VAR_STRING);
        // fields[i++].packetId = ++packetId;
        FIELDS[i] = PacketUtil.getField("STATE", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("SEND_QUEUE", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("SCHEMA", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("CHARACTER_SET_CLIENT", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("COLLATION_CONNECTION", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("CHARACTER_SET_RESULTS", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("TX_ISOLATION_LEVEL", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("AUTOCOMMIT", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("SYS_VARIABLES", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("USER_VARIABLES", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("XA_STATUS", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("DEAD_TIME", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        FIELDS[i] = PacketUtil.getField("USED_FOR_HEARTBEAT", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i].setPacketId(++packetId);

        EOF.setPacketId(++packetId);
    }

    private ShowBackend() {
    }

    public static void execute(ManagerService service, String whereCondition) {
        Map<String, String> whereInfo = new HashMap<>(8);
        if (!StringUtil.isEmpty(whereCondition)) {
            SQLStatement statement;
            try {
                statement = RouteStrategyFactory.getRouteStrategy().parserSQL("select 1 " + whereCondition);
                SQLExpr whereExpr = ((SQLSelectStatement) statement).getSelect().getQueryBlock().getWhere();
                ShowConnection.getWhereCondition(whereExpr, whereInfo);
            } catch (SQLSyntaxErrorException e) {
                service.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "The sql has error syntax.");
                return;
            }
        }

        ByteBuffer buffer = service.allocate();
        buffer = HEADER.write(buffer, service, true);
        for (FieldPacket field : FIELDS) {
            buffer = field.write(buffer, service, true);
        }
        buffer = EOF.write(buffer, service, true);
        byte packetId = EOF.getPacketId();

        IOProcessor[] processors = null;
        // return row
        if (!whereInfo.isEmpty() && whereInfo.get("processor") != null) {
            for (IOProcessor nioProcessor : DbleServer.getInstance().getBackendProcessors()) {
                if (nioProcessor.getName().equals(whereInfo.get("processor"))) {
                    processors = new IOProcessor[1];
                    processors[0] = nioProcessor;
                    whereInfo.remove("processor");
                    break;
                }
            }
        } else {
            processors = DbleServer.getInstance().getBackendProcessors();
        }

        if (processors == null) {
            EOFRowPacket lastEof = new EOFRowPacket();
            lastEof.setPacketId(++packetId);

            lastEof.write(buffer, service);
            return;
        }

        for (IOProcessor p : processors) {
            if (!whereInfo.isEmpty() && whereInfo.get("backend_id") != null) {
                BackendConnection bc = p.getBackends().get(Long.parseLong(whereInfo.get("backend_id")));
                if (bc == null) {
                    continue;
                }
                whereInfo.remove("backend_id");
                if (whereInfo.isEmpty() || checkConn(bc, whereInfo)) {
                    RowDataPacket row = getRow(bc, service.getCharset().getResults());
                    if (row != null) {
                        row.setPacketId(++packetId);
                        buffer = row.write(buffer, service, true);
                    }
                }
                break;
            }

            for (BackendConnection bc : p.getBackends().values()) {
                if (bc != null && (whereInfo.isEmpty() || checkConn(bc, whereInfo))) {
                    RowDataPacket row = getRow(bc, service.getCharset().getResults());
                    if (row != null) {
                        row.setPacketId(++packetId);
                        buffer = row.write(buffer, service, true);
                    }
                }
            }
        }

        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(++packetId);

        lastEof.write(buffer, service);
    }

    private static boolean checkConn(BackendConnection bc, Map<String, String> whereInfo) {
        boolean isMatch = true;
        if (whereInfo.get("host") != null) {
            isMatch = bc.getHost().equals(whereInfo.get("host"));
        }
        if (whereInfo.get("port") != null) {
            isMatch = isMatch && String.valueOf(bc.getPort()).equals(whereInfo.get("port"));
        }
        if (whereInfo.get("mysqlid") != null) {
            isMatch = isMatch && String.valueOf(bc.getThreadId()).equals(whereInfo.get("mysqlid"));
        }
        return isMatch;
    }

    private static RowDataPacket getRow(BackendConnection c, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        if (c.getService() instanceof AuthService) {
            return null;
        }
        int state = c.getState();
        row.add(c.getProcessor().getName().getBytes());
        row.add(LongUtil.toBytes(c.getId()));
        row.add(LongUtil.toBytes(c.getThreadId()));
        row.add(StringUtil.encode(c.getHost(), charset));
        row.add(IntegerUtil.toBytes(c.getPort()));
        row.add(IntegerUtil.toBytes(c.getLocalPort()));
        row.add(LongUtil.toBytes(c.getNetInBytes()));
        row.add(LongUtil.toBytes(c.getNetOutBytes()));
        row.add(LongUtil.toBytes((TimeUtil.currentTimeMillis() - c.getStartupTime()) / 1000L));
        row.add(c.isClosed() ? "true".getBytes() : "false".getBytes());
        row.add(stateStr(state).getBytes());
        row.add(IntegerUtil.toBytes(c.getWriteQueue().size()));
        row.add((c.getSchema() == null ? "NULL" : c.getSchema()).getBytes());
        row.add(c.getCharsetName().getClient().getBytes());
        row.add(c.getCharsetName().getCollation().getBytes());
        row.add(c.getCharsetName().getResults().getBytes());
        row.add((c.getBackendService().getTxIsolation() + "").getBytes());
        row.add((c.getBackendService().isAutocommit() + "").getBytes());
        row.add(StringUtil.encode(c.getBackendService().getStringOfSysVariables(), charset));
        row.add(StringUtil.encode(c.getBackendService().getStringOfUsrVariables(), charset));
        row.add(StringUtil.encode(c.getBackendService().getXaStatus().toString(), charset));
        row.add(StringUtil.encode(FormatUtil.formatDate(c.getPoolDestroyedTime()), charset));
        if (state == PooledConnection.INITIAL) {
            ResponseHandler handler = c.getBackendService().getResponseHandler();
            row.add(handler instanceof HeartbeatSQLJob ? "true".getBytes() : "false".getBytes());
        } else {
            row.add("false".getBytes());
        }
        return row;
    }

    public static String stateStr(int state) {
        switch (state) {
            case PooledConnection.STATE_IN_USE:
                return "IN USE";
            case PooledConnection.STATE_NOT_IN_USE:
                return "IDLE";
            case PooledConnection.STATE_REMOVED:
                return "REMOVED";
            case PooledConnection.STATE_HEARTBEAT:
                return "HEARTBEAT CHECK";
            case PooledConnection.STATE_RESERVED:
                return "EVICT";
            case PooledConnection.INITIAL:
                return "IN CREATION OR OUT OF POOL";
            default:
                return "UNKNOWN STATE";
        }
    }
}
