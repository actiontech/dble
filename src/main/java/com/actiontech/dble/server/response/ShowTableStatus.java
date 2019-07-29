/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.manager.handler.PackageBufINf;
import com.actiontech.dble.meta.SchemaMeta;
import com.actiontech.dble.meta.protocol.StructureMeta;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResultSetHeaderPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by szf on 2018/4/4.
 */
public final class ShowTableStatus {

    private static final String SHOW_TABLE_STATUS = "^\\s*(/\\*[\\s\\S]*\\*/)?\\s*(show)" +
            "(\\s+table)" +
            "(\\s+status)" +
            "(\\s+(from|in)\\s+(`?[a-zA-Z_0-9]+`?))?" +
            "((\\s+(like)\\s+'((. *)*)'\\s*)|(\\s+(where)\\s+((. *)*)\\s*))?" +
            "\\s*(/\\*[\\s\\S]*\\*/)?\\s*$";
    public static final Pattern PATTERN = Pattern.compile(SHOW_TABLE_STATUS, Pattern.CASE_INSENSITIVE);

    private ShowTableStatus() {
    }

    public static void response(ServerConnection c, String stmt) {

        Matcher ma = PATTERN.matcher(stmt);
        ma.matches(); //always RETURN TRUE
        String schema = ma.group(7);
        schema = schema == null ? null : StringUtil.removeBackQuote(schema);
        if (schema != null && DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
            schema = schema.toLowerCase();
        }
        String cSchema = schema == null ? c.getSchema() : schema;
        String likeCondition = ma.group(11);
        responseDirect(c, cSchema, likeCondition);
    }


    private static void responseDirect(ServerConnection c, String cSchema, String likeCondition) {
        if (cSchema == null) {
            c.writeErrMessage("3D000", "No database selected", ErrorCode.ER_NO_DB_ERROR);
            return;
        }
        SchemaMeta schemata = DbleServer.getInstance().getTmManager().getCatalogs().get(cSchema);
        if (schemata == null) {
            c.writeErrMessage("42000", "Unknown database " + cSchema, ErrorCode.ER_BAD_DB_ERROR);
            return;
        }
        ByteBuffer buffer = c.allocate();
        Map<String, StructureMeta.TableMeta> meta = schemata.getTableMetas();
        PackageBufINf bufInf;

        bufInf = writeTablesHeaderAndRows(buffer, c, meta, likeCondition);

        writeRowEof(bufInf.getBuffer(), c, bufInf.getPacketId());
    }

    private static void writeRowEof(ByteBuffer buffer, ServerConnection c, byte packetId) {
        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, c, true);

        // post write
        c.write(buffer);
    }

    private static PackageBufINf writeTablesHeaderAndRows(ByteBuffer buffer, ServerConnection c, Map<String, StructureMeta.TableMeta> tableMap, String likeCondition) {
        int fieldCount = 18;
        ResultSetHeaderPacket header = PacketUtil.getHeader(fieldCount);
        FieldPacket[] fields = new FieldPacket[fieldCount];
        int i = 0;
        byte packetId = 0;
        header.setPacketId(++packetId);
        packetId = getAllField(fields, packetId);

        EOFPacket eof = new EOFPacket();
        eof.setPacketId(++packetId);
        // write header
        buffer = header.write(buffer, c, true);
        // write fields
        for (FieldPacket field : fields) {
            buffer = field.write(buffer, c, true);
        }
        // write eof
        eof.write(buffer, c, true);

        Pattern pattern = null;
        if (likeCondition != null && !"".equals(likeCondition)) {
            String p = "^" + likeCondition.replaceAll("%", ".*");
            pattern = Pattern.compile(p, Pattern.CASE_INSENSITIVE);
        }
        for (String name : tableMap.keySet()) {
            RowDataPacket row = new RowDataPacket(fieldCount);
            if (DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
                name = name.toLowerCase();
            }
            if (pattern != null) {
                Matcher maLike = pattern.matcher(name);
                if (!maLike.matches()) {
                    continue;
                }
            }

            row.add(StringUtil.encode(name, c.getCharset().getResults()));
            row.add(StringUtil.encode("InnoDB", c.getCharset().getResults()));
            row.add(StringUtil.encode("10", c.getCharset().getResults()));
            row.add(StringUtil.encode("Compact", c.getCharset().getResults()));
            row.add(StringUtil.encode("0", c.getCharset().getResults()));
            row.add(StringUtil.encode("0", c.getCharset().getResults()));
            row.add(StringUtil.encode("16384", c.getCharset().getResults()));
            row.add(StringUtil.encode("0", c.getCharset().getResults()));
            row.add(StringUtil.encode("0", c.getCharset().getResults()));
            row.add(StringUtil.encode("0", c.getCharset().getResults()));
            row.add(StringUtil.encode("", c.getCharset().getResults()));
            row.add(StringUtil.encode("1970-01-01 00:00:00", c.getCharset().getResults()));
            row.add(StringUtil.encode("1970-01-01 00:00:00", c.getCharset().getResults()));
            row.add(StringUtil.encode("1970-01-01 00:00:00", c.getCharset().getResults()));
            row.add(StringUtil.encode("utf8_general_ci", c.getCharset().getResults()));
            row.add(StringUtil.encode("", c.getCharset().getResults()));
            row.add(StringUtil.encode("", c.getCharset().getResults()));
            row.add(StringUtil.encode("", c.getCharset().getResults()));
            row.setPacketId(++packetId);
            buffer = row.write(buffer, c, true);
        }
        PackageBufINf packBuffInfo = new PackageBufINf();
        packBuffInfo.setBuffer(buffer);
        packBuffInfo.setPacketId(packetId);
        return packBuffInfo;
    }


    private static byte getAllField(FieldPacket[] fields, byte packetId) {
        int i = 0;
        fields[i] = PacketUtil.getField("Name", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].setPacketId(++packetId);

        fields[i] = PacketUtil.getField("Engine", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].setPacketId(++packetId);

        fields[i] = PacketUtil.getField("Version", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].setPacketId(++packetId);

        fields[i] = PacketUtil.getField("Row_format", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].setPacketId(++packetId);

        fields[i] = PacketUtil.getField("Rows", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].setPacketId(++packetId);

        fields[i] = PacketUtil.getField("Avg_row_length", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].setPacketId(++packetId);

        fields[i] = PacketUtil.getField("Data_length", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].setPacketId(++packetId);

        fields[i] = PacketUtil.getField("Max_data_length", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].setPacketId(++packetId);

        fields[i] = PacketUtil.getField("Index_length", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].setPacketId(++packetId);

        fields[i] = PacketUtil.getField("Data_free", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].setPacketId(++packetId);

        fields[i] = PacketUtil.getField("Auto_increment", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].setPacketId(++packetId);

        fields[i] = PacketUtil.getField("Create_time", Fields.FIELD_TYPE_DATE);
        fields[i++].setPacketId(++packetId);

        fields[i] = PacketUtil.getField("Update_time", Fields.FIELD_TYPE_DATE);
        fields[i++].setPacketId(++packetId);


        fields[i] = PacketUtil.getField("Check_time", Fields.FIELD_TYPE_DATE);
        fields[i++].setPacketId(++packetId);

        fields[i] = PacketUtil.getField("Collation", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].setPacketId(++packetId);

        fields[i] = PacketUtil.getField("Checksum", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].setPacketId(++packetId);

        fields[i] = PacketUtil.getField("Create_options", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].setPacketId(++packetId);

        fields[i] = PacketUtil.getField("Comment", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].setPacketId(++packetId);

        return packetId;
    }


}
