/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.response;

import com.oceanbase.obsharding_d.backend.mysql.PacketUtil;
import com.oceanbase.obsharding_d.config.Fields;
import com.oceanbase.obsharding_d.net.mysql.*;
import com.oceanbase.obsharding_d.services.manager.ManagerService;
import com.oceanbase.obsharding_d.statistic.stat.TableStat;
import com.oceanbase.obsharding_d.statistic.stat.TableStatAnalyzer;
import com.oceanbase.obsharding_d.util.FormatUtil;
import com.oceanbase.obsharding_d.util.LongUtil;
import com.oceanbase.obsharding_d.util.StringUtil;

import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.List;

public final class ShowSQLSumTable {
    private ShowSQLSumTable() {
    }

    private static DecimalFormat decimalFormat = new DecimalFormat("0.00");

    private static final int FIELD_COUNT = 8;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("ID", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("TABLE", Fields.FIELD_TYPE_VARCHAR);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("R", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("W", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("R%", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("RELATABLE", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("RELACOUNT", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("LAST_TIME", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].setPacketId(++packetId);
        EOF.setPacketId(++packetId);
    }

    public static void execute(ManagerService service, boolean isClear) {
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

        /*
        int i=0;
        Map<String, TableStat> statMap = TableStatAnalyzer.getInstance().getTableStatMap();
        for (TableStat tableStat : statMap.values()) {
            i++;
           RowDataPacket row = getRow(tableStat,i, c.getCharset());//getRow(sqlStat,sql, c.getCharset());
           row.packetId = ++packetId;
           buffer = row.write(buffer, c,true);
        }
        */
        List<TableStat> list = TableStatAnalyzer.getInstance().getTableStats(isClear);
        int i = 1;
        for (TableStat tableStat : list) {
            if (tableStat != null) {
                RowDataPacket row = getRow(tableStat, i, service.getCharset().getResults());
                i++;
                row.setPacketId(++packetId);
                buffer = row.write(buffer, service, true);
            }
        }
        // write last eof
        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(++packetId);


        lastEof.write(buffer, service);
    }

    private static RowDataPacket getRow(TableStat tableStat, long idx, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(LongUtil.toBytes(idx));
        if (tableStat == null) {
            row.add(StringUtil.encode("not found", charset));
            return row;
        }

        String table = tableStat.getTable();

        StringBuilder relationTableNameBuffer = new StringBuilder();
        StringBuilder relationTableCountBuffer = new StringBuilder();
        List<TableStat.RelationTable> relationTables = tableStat.getRelationTables();
        if (!relationTables.isEmpty()) {

            for (TableStat.RelationTable relationTable : relationTables) {
                relationTableNameBuffer.append(relationTable.getTableName()).append(", ");
                relationTableCountBuffer.append(relationTable.getCount()).append(", ");
            }

        } else {
            relationTableNameBuffer.append("NULL");
            relationTableCountBuffer.append("NULL");
        }

        row.add(StringUtil.encode(table, charset));

        long r = tableStat.getRCount();
        long w = tableStat.getWCount();
        row.add(LongUtil.toBytes(r));
        row.add(LongUtil.toBytes(w));

        String rStr = decimalFormat.format(1.0D * r / (r + w));
        row.add(StringUtil.encode(String.valueOf(rStr), charset));
        row.add(StringUtil.encode(relationTableNameBuffer.toString(), charset));
        row.add(StringUtil.encode(relationTableCountBuffer.toString(), charset));
        row.add(StringUtil.encode(FormatUtil.formatDate(tableStat.getLastExecuteTime()), charset));

        return row;
    }

}
