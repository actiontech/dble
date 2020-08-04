/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.sharding.table.*;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.singleton.ProxyMeta;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

import static com.actiontech.dble.route.parser.ManagerParseShow.PATTERN_FOR_TABLE_INFO;

public final class ShowTableAlgorithm {
    private ShowTableAlgorithm() {
    }

    private static final int FIELD_COUNT = 2;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("KEY", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("VALUE", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i].setPacketId(++packetId);

        EOF.setPacketId(++packetId);
    }

    private enum TableType {
        GLOBAL, SHARDING, CHILD, BASE, SINGLE
    }

    public static void execute(ManagerService service, String tableInfo) {
        Matcher ma = PATTERN_FOR_TABLE_INFO.matcher(tableInfo);
        if (!ma.matches()) {
            service.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "The Correct Query Format Is:show @@algorithm where schema='?' and table ='?'");
            return;
        }
        String schemaName = StringUtil.removeAllApostrophe(ma.group(1));
        String tableName = StringUtil.removeAllApostrophe(ma.group(5));
        if (DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
            schemaName = schemaName.toLowerCase();
            tableName = tableName.toLowerCase();
        }

        SchemaConfig schemaConfig = DbleServer.getInstance().getConfig().getSchemas().get(schemaName);
        TableType tableType = null;
        BaseTableConfig tableConfig = null;
        if (schemaConfig == null) {
            service.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "the schema [" + schemaName + "] does not exists");
            return;
        } else if (schemaConfig.isNoSharding()) {
            if (ProxyMeta.getInstance().getTmManager().checkTableExists(schemaName, tableName)) {
                tableType = TableType.BASE;
            }
        } else {
            tableConfig = schemaConfig.getTables().get(tableName);
            if (tableConfig == null) {
                if (schemaConfig.getShardingNode() == null) {
                    service.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "the table [" + tableName + "] in schema [" + schemaName + "] does not exists");
                    return;
                } else if (ProxyMeta.getInstance().getTmManager().checkTableExists(schemaName, tableName)) {
                    tableType = TableType.BASE;
                }
            } else if (tableConfig instanceof GlobalTableConfig) {
                tableType = TableType.GLOBAL;
            } else if (tableConfig instanceof ChildTableConfig) {
                tableType = TableType.CHILD;
            } else if (tableConfig instanceof SingleTableConfig) {
                tableType = TableType.SINGLE;
            } else {
                tableType = TableType.SHARDING;
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

        if (tableType != null) {
            for (RowDataPacket row : getRows(tableConfig, tableType, service.getCharset().getResults())) {
                row.setPacketId(++packetId);
                buffer = row.write(buffer, service, true);
            }
        }

        // write last eof
        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(++packetId);


        lastEof.write(buffer, service);
    }

    private static List<RowDataPacket> getRows(BaseTableConfig tableConfig, TableType tableType, String charset) {
        List<RowDataPacket> list = new ArrayList<>();
        switch (tableType) {
            case GLOBAL: {
                RowDataPacket row = new RowDataPacket(FIELD_COUNT);
                row.add(StringUtil.encode("TYPE", charset));
                row.add(StringUtil.encode("GLOBAL TABLE", charset));
                list.add(row);
                break;
            }
            case CHILD: {
                RowDataPacket row = new RowDataPacket(FIELD_COUNT);
                row.add(StringUtil.encode("TYPE", charset));
                row.add(StringUtil.encode("CHILD TABLE", charset));
                list.add(row);
                break;
            }
            case BASE: {
                RowDataPacket row = new RowDataPacket(FIELD_COUNT);
                row.add(StringUtil.encode("TYPE", charset));
                row.add(StringUtil.encode("BASE TABLE", charset));
                list.add(row);
                break;
            }
            case SINGLE: {
                RowDataPacket row = new RowDataPacket(FIELD_COUNT);
                row.add(StringUtil.encode("TYPE", charset));
                row.add(StringUtil.encode("SINGLE TABLE", charset));
                list.add(row);
                break;
            }
            case SHARDING: {
                RowDataPacket typeRow = new RowDataPacket(FIELD_COUNT);
                typeRow.add(StringUtil.encode("TYPE", charset));
                typeRow.add(StringUtil.encode("SHARDING TABLE", charset));
                list.add(typeRow);

                ShardingTableConfig shardingTableConfig = (ShardingTableConfig) tableConfig;
                RowDataPacket columnRow = new RowDataPacket(FIELD_COUNT);
                columnRow.add(StringUtil.encode("COLUMN", charset));
                columnRow.add(StringUtil.encode(shardingTableConfig.getShardingColumn(), charset));
                list.add(columnRow);
                RowDataPacket classRow = new RowDataPacket(FIELD_COUNT);
                classRow.add(StringUtil.encode("CLASS", charset));
                classRow.add(StringUtil.encode(shardingTableConfig.getFunction().getClass().getName(), charset));
                list.add(classRow);
                for (Map.Entry<String, String> pairs : shardingTableConfig.getFunction().getAllProperties().entrySet()) {
                    RowDataPacket propertyRow = new RowDataPacket(FIELD_COUNT);
                    propertyRow.add(StringUtil.encode(pairs.getKey(), charset));
                    propertyRow.add(StringUtil.encode(pairs.getValue(), charset));
                    list.add(propertyRow);
                }
                break;
            }
            default:
                break;
        }
        return list;
    }
}
