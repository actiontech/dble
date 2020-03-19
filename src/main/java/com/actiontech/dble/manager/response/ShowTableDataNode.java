/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDataNode;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.model.DataSourceConfig;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.singleton.ProxyMeta;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResultSetHeaderPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.util.LongUtil;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;

import static com.actiontech.dble.route.parser.ManagerParseShow.PATTERN_FOR_TABLE_INFO;

public final class ShowTableDataNode {
    private ShowTableDataNode() {
    }

    private static final int FIELD_COUNT = 7;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("NAME", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("SEQUENCE", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("HOST", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("PORT", Fields.FIELD_TYPE_LONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("PHYSICAL_SCHEMA", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("USER", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("PASSWORD", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i].setPacketId(++packetId);

        EOF.setPacketId(++packetId);
    }

    public static void execute(ManagerConnection c, String tableInfo) {
        Matcher ma = PATTERN_FOR_TABLE_INFO.matcher(tableInfo);
        if (!ma.matches()) {
            c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "The Correct Query Format Is:show @@datanodes where schema='?' and table ='?'");
            return;
        }
        String schemaName = StringUtil.removeAllApostrophe(ma.group(1));
        String tableName = StringUtil.removeAllApostrophe(ma.group(5));
        if (DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
            schemaName = schemaName.toLowerCase();
            tableName = tableName.toLowerCase();
        }

        SchemaConfig schemaConfig = DbleServer.getInstance().getConfig().getSchemas().get(schemaName);
        List<String> dataNodes = null;
        if (schemaConfig == null) {
            c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "the schema [" + schemaName + "] does not exists");
            return;
        } else if (schemaConfig.isNoSharding()) {
            if (ProxyMeta.getInstance().getTmManager().checkTableExists(schemaName, tableName)) {
                dataNodes = Collections.singletonList(schemaConfig.getDataNode());
            }
        } else {
            TableConfig tableConfig = schemaConfig.getTables().get(tableName);
            if (tableConfig == null) {
                if (schemaConfig.getDataNode() == null) {
                    c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "the table [" + tableName + "] in schema [" + schemaName + "] does not exists");
                    return;
                } else if (ProxyMeta.getInstance().getTmManager().checkTableExists(schemaName, tableName)) {
                    dataNodes = Collections.singletonList(schemaConfig.getDataNode());
                }
            } else {
                dataNodes = tableConfig.getDataNodes();
            }
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

        if (dataNodes != null) {
            for (RowDataPacket row : getRows(dataNodes, c.getCharset().getResults())) {
                row.setPacketId(++packetId);
                buffer = row.write(buffer, c, true);
            }
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, c, true);

        // post write
        c.write(buffer);
    }

    private static List<RowDataPacket> getRows(List<String> dataNodes, String charset) {
        List<RowDataPacket> list = new ArrayList<>();
        int sequence = 0;
        for (String dataNode : dataNodes) {
            PhysicalDataNode dn = DbleServer.getInstance().getConfig().getDataNodes().get(dataNode);
            DataSourceConfig dbConfig = dn.getDataHost().getWriteSource().getConfig();
            RowDataPacket row = new RowDataPacket(FIELD_COUNT);
            row.add(StringUtil.encode(dn.getName(), charset));
            row.add(LongUtil.toBytes(sequence));
            row.add(StringUtil.encode(dbConfig.getIp(), charset));
            row.add(LongUtil.toBytes(dbConfig.getPort()));
            row.add(StringUtil.encode(dn.getDatabase(), charset));
            row.add(StringUtil.encode(dbConfig.getUser(), charset));
            row.add(StringUtil.encode(dbConfig.getPassword(), charset));
            list.add(row);
            sequence++;
        }
        return list;
    }
}
