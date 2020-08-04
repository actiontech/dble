/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.ShardingNode;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.model.db.DbInstanceConfig;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.sharding.table.BaseTableConfig;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.singleton.ProxyMeta;
import com.actiontech.dble.util.LongUtil;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;

import static com.actiontech.dble.route.parser.ManagerParseShow.PATTERN_FOR_TABLE_INFO;

public final class ShowTableShardingNode {
    private ShowTableShardingNode() {
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

    public static void execute(ManagerService service, String tableInfo) {
        Matcher ma = PATTERN_FOR_TABLE_INFO.matcher(tableInfo);
        if (!ma.matches()) {
            service.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "The Correct Query Format Is:show @@shardingnodes where schema='?' and table ='?'");
            return;
        }
        String schemaName = StringUtil.removeAllApostrophe(ma.group(1));
        String tableName = StringUtil.removeAllApostrophe(ma.group(5));
        if (DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
            schemaName = schemaName.toLowerCase();
            tableName = tableName.toLowerCase();
        }

        SchemaConfig schemaConfig = DbleServer.getInstance().getConfig().getSchemas().get(schemaName);
        List<String> shardingNodes = null;
        if (schemaConfig == null) {
            service.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "the schema [" + schemaName + "] does not exists");
            return;
        } else if (schemaConfig.isNoSharding()) {
            if (ProxyMeta.getInstance().getTmManager().checkTableExists(schemaName, tableName)) {
                shardingNodes = Collections.singletonList(schemaConfig.getShardingNode());
            }
        } else {
            BaseTableConfig tableConfig = schemaConfig.getTables().get(tableName);
            if (tableConfig == null) {
                if (schemaConfig.getShardingNode() == null) {
                    service.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "the table [" + tableName + "] in schema [" + schemaName + "] does not exists");
                    return;
                } else if (ProxyMeta.getInstance().getTmManager().checkTableExists(schemaName, tableName)) {
                    shardingNodes = Collections.singletonList(schemaConfig.getShardingNode());
                }
            } else {
                shardingNodes = tableConfig.getShardingNodes();
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

        if (shardingNodes != null) {
            for (RowDataPacket row : getRows(shardingNodes, service.getCharset().getResults())) {
                row.setPacketId(++packetId);
                buffer = row.write(buffer, service, true);
            }
        }

        // write last eof
        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(++packetId);


        lastEof.write(buffer, service);
    }

    private static List<RowDataPacket> getRows(List<String> shardingNodes, String charset) {
        List<RowDataPacket> list = new ArrayList<>();
        int sequence = 0;
        for (String shardingNode : shardingNodes) {
            ShardingNode dn = DbleServer.getInstance().getConfig().getShardingNodes().get(shardingNode);
            DbInstanceConfig dbConfig = dn.getDbGroup().getWriteDbInstance().getConfig();
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
