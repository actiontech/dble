/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.config.model.rule.RuleConfig;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResultSetHeaderPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
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
        GLOBAL, SHARDING, CHILD, BASE, SHARDING_SINGLE
    }

    public static void execute(ManagerConnection c, String tableInfo) {
        Matcher ma = PATTERN_FOR_TABLE_INFO.matcher(tableInfo);
        if (!ma.matches()) {
            c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "The Correct Query Format Is:show @@algorithm where schema=? and table =?");
            return;
        }
        String schemaName = ma.group(2);
        String tableName = ma.group(4);
        if (DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
            schemaName = schemaName.toLowerCase();
            tableName = tableName.toLowerCase();
        }

        SchemaConfig schemaConfig = DbleServer.getInstance().getConfig().getSchemas().get(schemaName);
        TableType tableType = null;
        TableConfig tableConfig = null;
        if (schemaConfig == null) {
            c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "the schema [" + schemaName + "] does not exists");
            return;
        } else if (schemaConfig.isNoSharding()) {
            if (DbleServer.getInstance().getTmManager().checkTableExists(schemaName, tableName)) {
                tableType = TableType.BASE;
            }
        } else {
            tableConfig = schemaConfig.getTables().get(tableName);
            if (tableConfig == null) {
                if (schemaConfig.getDataNode() == null) {
                    c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "the table [" + tableName + "] in schema [" + schemaName + "] does not exists");
                    return;
                } else if (DbleServer.getInstance().getTmManager().checkTableExists(schemaName, tableName)) {
                    tableType = TableType.BASE;
                }
            } else if (tableConfig.isGlobalTable()) {
                tableType = TableType.GLOBAL;
            } else if (tableConfig.getParentTC() != null) {
                tableType = TableType.CHILD;
            } else if (tableConfig.getRule() == null) {
                tableType = TableType.SHARDING_SINGLE;
            } else {
                tableType = TableType.SHARDING;
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

        if (tableType != null) {
            for (RowDataPacket row : getRows(tableConfig, tableType, c.getCharset().getResults())) {
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

    private static List<RowDataPacket> getRows(TableConfig tableConfig, TableType tableType, String charset) {
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
            case SHARDING_SINGLE: {
                RowDataPacket row = new RowDataPacket(FIELD_COUNT);
                row.add(StringUtil.encode("TYPE", charset));
                row.add(StringUtil.encode("SINGLE SHARDING TABLE", charset));
                list.add(row);
                break;
            }
            case SHARDING: {
                RowDataPacket typeRow = new RowDataPacket(FIELD_COUNT);
                typeRow.add(StringUtil.encode("TYPE", charset));
                typeRow.add(StringUtil.encode("SHARDING TABLE", charset));
                list.add(typeRow);

                RuleConfig rule = tableConfig.getRule();
                RowDataPacket columnRow = new RowDataPacket(FIELD_COUNT);
                columnRow.add(StringUtil.encode("COLUMN", charset));
                columnRow.add(StringUtil.encode(rule.getColumn(), charset));
                list.add(columnRow);
                RowDataPacket classRow = new RowDataPacket(FIELD_COUNT);
                classRow.add(StringUtil.encode("CLASS", charset));
                classRow.add(StringUtil.encode(rule.getRuleAlgorithm().getClass().getName(), charset));
                list.add(classRow);
                for (Map.Entry<String, String> pairs : rule.getRuleAlgorithm().getAllProperties().entrySet()) {
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
