/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.net.IOProcessor;
import com.actiontech.dble.net.connection.FrontendConnection;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.route.factory.RouteStrategyFactory;
import com.actiontech.dble.services.VariablesService;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.util.IntegerUtil;
import com.actiontech.dble.util.LongUtil;
import com.actiontech.dble.util.StringUtil;
import com.actiontech.dble.util.TimeUtil;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;

import java.nio.ByteBuffer;
import java.sql.SQLSyntaxErrorException;
import java.util.HashMap;
import java.util.Map;

/**
 * Show Active Connection
 *
 * @author mycat
 */
public final class ShowConnection {
    private static final int FIELD_COUNT = 19;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("PROCESSOR", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("FRONT_ID", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("HOST", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("PORT", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("LOCAL_PORT", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("USER", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("SCHEMA", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("CHARACTER_SET_CLIENT", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("COLLATION_CONNECTION", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("CHARACTER_SET_RESULTS", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("NET_IN", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("NET_OUT", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("ALIVE_TIME(S)", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("RECV_BUFFER", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("SEND_QUEUE", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("TX_ISOLATION_LEVEL", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("AUTOCOMMIT", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("SYS_VARIABLES", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("USER_VARIABLES", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i].setPacketId(++packetId);

        EOF.setPacketId(++packetId);
    }

    private ShowConnection() {
    }

    public static void execute(ManagerService service, String whereCondition) {
        Map<String, String> whereInfo = new HashMap<>(8);
        if (!StringUtil.isEmpty(whereCondition)) {
            SQLStatement statement;
            try {
                statement = RouteStrategyFactory.getRouteStrategy().parserSQL("select 1 " + whereCondition);
                SQLExpr whereExpr = ((SQLSelectStatement) statement).getSelect().getQueryBlock().getWhere();
                getWhereCondition(whereExpr, whereInfo);
            } catch (SQLSyntaxErrorException e) {
                service.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "The sql has error syntax.");
                return;
            }
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
        IOProcessor[] processors = null;

        if (!whereInfo.isEmpty() && whereInfo.get("processor") != null) {
            for (IOProcessor nioProcessor : DbleServer.getInstance().getFrontProcessors()) {
                if (nioProcessor.getName().equals(whereInfo.get("processor"))) {
                    processors = new IOProcessor[1];
                    processors[0] = nioProcessor;
                    whereInfo.remove("processor");
                    break;
                }
            }
        } else {
            processors = DbleServer.getInstance().getFrontProcessors();
        }

        if (processors == null) {
            EOFRowPacket lastEof = new EOFRowPacket();
            lastEof.setPacketId(++packetId);

            lastEof.write(buffer, service);
            return;
        }

        for (IOProcessor p : processors) {
            if (!whereInfo.isEmpty() && whereInfo.get("front_id") != null) {
                FrontendConnection fc = p.getFrontends().get(Long.parseLong(whereInfo.get("front_id")));
                if (fc == null) {
                    continue;
                }
                whereInfo.remove("front_id");
                if (whereInfo.isEmpty() || checkConn(fc, whereInfo)) {
                    RowDataPacket row = getRow(fc, service.getCharset().getResults());
                    row.setPacketId(++packetId);
                    buffer = row.write(buffer, service, true);
                }
                break;
            }

            for (FrontendConnection fc : p.getFrontends().values()) {
                if (fc != null && (whereInfo.isEmpty() || checkConn(fc, whereInfo))) {
                    RowDataPacket row = getRow(fc, service.getCharset().getResults());
                    row.setPacketId(++packetId);
                    buffer = row.write(buffer, service, true);
                }
            }
        }

        // write last eof
        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(++packetId);
        lastEof.write(buffer, service);
    }

    public static void getWhereCondition(SQLExpr whereExpr, Map<String, String> whereInfo) {
        if (whereExpr instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr tmp = (SQLBinaryOpExpr) whereExpr;
            if (tmp.getLeft() instanceof SQLBinaryOpExpr) {
                getWhereCondition(tmp.getLeft(), whereInfo);
                getWhereCondition(tmp.getRight(), whereInfo);
            } else {
                whereInfo.put(tmp.getLeft().toString().toLowerCase(), StringUtil.removeApostrophe(tmp.getRight().toString()));
            }
        }
    }

    private static boolean checkConn(FrontendConnection fc, Map<String, String> whereInfo) {
        boolean isMatch = true;
        if (whereInfo.get("host") != null) {
            isMatch = fc.getHost().equals(whereInfo.get("host"));
        }
        if (whereInfo.get("user") != null) {
            isMatch = isMatch && fc.getFrontEndService().getUser().toString().equals(whereInfo.get("user"));
        }
        return isMatch;
    }

    private static RowDataPacket getRow(FrontendConnection c, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(c.getProcessor().getName().getBytes());
        row.add(LongUtil.toBytes(c.getId()));
        row.add(StringUtil.encode(c.getHost(), charset));
        row.add(IntegerUtil.toBytes(c.getPort()));
        row.add(IntegerUtil.toBytes(c.getLocalPort()));
        row.add(StringUtil.encode(c.getFrontEndService().getUser().toString(), charset));
        if (!c.isManager()) {
            row.add(StringUtil.encode(((ShardingService) c.getService()).getSchema(), charset));
        } else {
            row.add(StringUtil.encode("", charset));
        }
        row.add(StringUtil.encode(c.getCharsetName().getClient(), charset));
        row.add(StringUtil.encode(c.getCharsetName().getCollation(), charset));
        row.add(StringUtil.encode(c.getCharsetName().getResults(), charset));
        row.add(LongUtil.toBytes(c.getNetInBytes()));
        row.add(LongUtil.toBytes(c.getNetOutBytes()));
        row.add(LongUtil.toBytes((TimeUtil.currentTimeMillis() - c.getStartupTime()) / 1000L));
        ByteBuffer bb = c.getReadBuffer();
        row.add(IntegerUtil.toBytes(bb == null ? 0 : bb.capacity()));
        row.add(IntegerUtil.toBytes(c.getWriteQueue().size()));

        String txLevel = "";
        String autocommit = "";
        if (!c.isManager()) {
            ShardingService shardingService = (ShardingService) c.getService();
            txLevel = shardingService.getTxIsolation() + "";
            autocommit = shardingService.isAutocommit() + "";
        }
        row.add(txLevel.getBytes());
        row.add(autocommit.getBytes());
        row.add(StringUtil.encode(((VariablesService) c.getService()).getStringOfSysVariables(), charset));
        row.add(StringUtil.encode(((VariablesService) c.getService()).getStringOfUsrVariables(), charset));
        return row;
    }

}
