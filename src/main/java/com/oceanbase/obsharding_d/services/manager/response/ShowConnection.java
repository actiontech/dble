/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.services.manager.response;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.mysql.PacketUtil;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.config.Fields;
import com.oceanbase.obsharding_d.net.IOProcessor;
import com.oceanbase.obsharding_d.net.connection.FrontendConnection;
import com.oceanbase.obsharding_d.net.mysql.*;
import com.oceanbase.obsharding_d.route.parser.util.DruidUtil;
import com.oceanbase.obsharding_d.services.FrontendService;
import com.oceanbase.obsharding_d.services.manager.ManagerService;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;
import com.oceanbase.obsharding_d.util.IntegerUtil;
import com.oceanbase.obsharding_d.util.LongUtil;
import com.oceanbase.obsharding_d.util.StringUtil;
import com.oceanbase.obsharding_d.util.TimeUtil;
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
    private static final int FIELD_COUNT = 21;
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

        FIELDS[i] = PacketUtil.getField("RECV_QUEUE", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("TX_ISOLATION_LEVEL", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("AUTOCOMMIT", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("SYS_VARIABLES", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("USER_VARIABLES", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("XA_ID", Fields.FIELD_TYPE_VAR_STRING);
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
                statement = DruidUtil.parseMultiSQL("select 1 " + whereCondition);
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
            for (IOProcessor nioProcessor : OBsharding_DServer.getInstance().getFrontProcessors()) {
                if (nioProcessor.getName().equals(whereInfo.get("processor"))) {
                    processors = new IOProcessor[1];
                    processors[0] = nioProcessor;
                    whereInfo.remove("processor");
                    break;
                }
            }
        } else {
            processors = OBsharding_DServer.getInstance().getFrontProcessors();
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
                if (fc.isAuthorized() && (whereInfo.isEmpty() || checkConn(fc, whereInfo))) {
                    RowDataPacket row = getRow(fc, service.getCharset().getResults());
                    row.setPacketId(++packetId);
                    buffer = row.write(buffer, service, true);
                }
                break;
            }

            for (FrontendConnection fc : p.getFrontends().values()) {
                if (fc != null && fc.isAuthorized() && (whereInfo.isEmpty() || checkConn(fc, whereInfo))) {
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
            isMatch = isMatch && fc.getFrontEndService().getUser().getFullName().equals(whereInfo.get("user"));
        }
        return isMatch;
    }

    private static RowDataPacket getRow(FrontendConnection c, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        final FrontendService service = c.getFrontEndService();
        row.add(c.getProcessor().getName().getBytes());
        row.add(LongUtil.toBytes(c.getId()));
        row.add(StringUtil.encode(c.getHost(), charset));
        row.add(IntegerUtil.toBytes(c.getPort()));
        row.add(IntegerUtil.toBytes(c.getLocalPort()));
        row.add(StringUtil.encode(service.getUser().getFullName(), charset));
        row.add(StringUtil.encode(service.getSchema(), charset));
        row.add(StringUtil.encode(service.getCharset().getClient(), charset));
        row.add(StringUtil.encode(service.getCharset().getCollation(), charset));
        row.add(StringUtil.encode(service.getCharset().getResults(), charset));
        row.add(LongUtil.toBytes(c.getNetInBytes()));
        row.add(LongUtil.toBytes(c.getNetOutBytes()));
        row.add(LongUtil.toBytes((TimeUtil.currentTimeMillis() - c.getStartupTime()) / 1000L));
        ByteBuffer bb = c.getBottomReadBuffer();
        row.add(IntegerUtil.toBytes(bb == null ? 0 : bb.capacity()));
        row.add(IntegerUtil.toBytes(c.getWriteQueue().size()));
        row.add(IntegerUtil.toBytes(service.getRecvTaskQueueSize()));
        String txLevel = "";
        String autocommit = "";
        if (!c.isManager()) {
            txLevel = service.getTxIsolation() + "";
            autocommit = service.isAutocommit() + "";
        }
        row.add(txLevel.getBytes());
        row.add(autocommit.getBytes());
        row.add(StringUtil.encode(service.getStringOfSysVariables(), charset));
        row.add(StringUtil.encode(service.getStringOfUsrVariables(), charset));
        if (service instanceof ShardingService) {
            String xaid = ((ShardingService) service).getSession2().getSessionXaID();
            row.add(StringUtil.encode(xaid == null ? "NULL" : xaid, charset));
        } else {
            row.add(StringUtil.encode("-", charset));
        }
        return row;
    }

}
