/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.server.response;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.mysql.PacketUtil;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.config.Fields;
import com.oceanbase.obsharding_d.meta.SchemaMeta;
import com.oceanbase.obsharding_d.meta.TableMeta;
import com.oceanbase.obsharding_d.net.mysql.*;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;
import com.oceanbase.obsharding_d.singleton.ProxyMeta;
import com.oceanbase.obsharding_d.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by szf on 2018/4/4.
 */
public final class ShowTableStatus {

    private static final String SHOW_TABLE_STATUS = "^\\s*(/\\*[\\s\\S]*\\*/)?\\s*(show)" +
            "(\\s+table)" +
            "(\\s+status)" +
            "(\\s+(from|in)\\s+((`((?!`).)+`|[a-zA-Z_0-9]+)))?" +
            "((\\s+(like)\\s+'((. *)*)'\\s*)|(\\s+(where)\\s+((. *)*)\\s*))?" +
            "\\s*(/\\*[\\s\\S]*\\*/)?\\s*$";
    public static final Pattern PATTERN = Pattern.compile(SHOW_TABLE_STATUS, Pattern.CASE_INSENSITIVE);

    private ShowTableStatus() {
    }

    public static void response(ShardingService shardingService, String stmt) {

        Matcher ma = PATTERN.matcher(stmt);
        ma.matches(); //always RETURN TRUE
        String schema = ma.group(7);
        schema = schema == null ? null : StringUtil.removeBackQuote(schema);
        if (schema != null && OBsharding_DServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
            schema = schema.toLowerCase();
        }
        String cSchema = schema == null ? shardingService.getSchema() : schema;
        String likeCondition = ma.group(13);
        responseDirect(shardingService, cSchema, likeCondition);
    }


    private static void responseDirect(ShardingService service, String cSchema, String likeCondition) {
        if (cSchema == null) {
            service.writeErrMessage("3D000", "No database selected", ErrorCode.ER_NO_DB_ERROR);
            return;
        }
        SchemaMeta schemata = ProxyMeta.getInstance().getTmManager().getCatalogs().get(cSchema);
        if (schemata == null) {
            service.writeErrMessage("42000", "Unknown database " + cSchema, ErrorCode.ER_BAD_DB_ERROR);
            return;
        }
        ByteBuffer buffer = service.allocate();
        Map<String, TableMeta> meta = schemata.getTableMetas();
        PackageBufINf bufInf;

        bufInf = writeTablesHeaderAndRows(buffer, service, meta, likeCondition);

        writeRowEof(bufInf.getBuffer(), service, bufInf.getPacketId());
    }

    private static void writeRowEof(ByteBuffer buffer, ShardingService shardingService, byte packetId) {
        // writeDirectly last eof
        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(++packetId);
        lastEof.write(buffer, shardingService);
    }

    private static PackageBufINf writeTablesHeaderAndRows(ByteBuffer buffer, ShardingService service, Map<String, TableMeta> tableMap, String likeCondition) {
        int fieldCount = 18;
        ResultSetHeaderPacket header = PacketUtil.getHeader(fieldCount);
        FieldPacket[] fields = new FieldPacket[fieldCount];
        int i = 0;
        byte packetId = 0;
        header.setPacketId(++packetId);
        packetId = getAllField(fields, packetId);

        EOFPacket eof = new EOFPacket();
        eof.setPacketId(++packetId);
        // writeDirectly header
        buffer = header.write(buffer, service, true);
        // writeDirectly fields
        for (FieldPacket field : fields) {
            buffer = field.write(buffer, service, true);
        }
        // writeDirectly eof
        eof.write(buffer, service, true);

        Pattern pattern = null;
        if (likeCondition != null && !"".equals(likeCondition)) {
            String p = "^" + likeCondition.replaceAll("%", ".*");
            pattern = Pattern.compile(p, Pattern.CASE_INSENSITIVE);
        }
        for (String name : tableMap.keySet()) {
            RowDataPacket row = new RowDataPacket(fieldCount);
            if (OBsharding_DServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
                name = name.toLowerCase();
            }
            if (pattern != null) {
                Matcher maLike = pattern.matcher(name);
                if (!maLike.matches()) {
                    continue;
                }
            }

            row.add(StringUtil.encode(name, service.getCharset().getResults()));
            row.add(StringUtil.encode("InnoDB", service.getCharset().getResults()));
            row.add(StringUtil.encode("10", service.getCharset().getResults()));
            row.add(StringUtil.encode("Compact", service.getCharset().getResults()));
            row.add(StringUtil.encode("0", service.getCharset().getResults()));
            row.add(StringUtil.encode("0", service.getCharset().getResults()));
            row.add(StringUtil.encode("16384", service.getCharset().getResults()));
            row.add(StringUtil.encode("0", service.getCharset().getResults()));
            row.add(StringUtil.encode("0", service.getCharset().getResults()));
            row.add(StringUtil.encode("0", service.getCharset().getResults()));
            row.add(StringUtil.encode("", service.getCharset().getResults()));
            row.add(StringUtil.encode("1970-01-01 00:00:00", service.getCharset().getResults()));
            row.add(StringUtil.encode("1970-01-01 00:00:00", service.getCharset().getResults()));
            row.add(StringUtil.encode("1970-01-01 00:00:00", service.getCharset().getResults()));
            row.add(StringUtil.encode("utf8mb4_general_ci", service.getCharset().getResults()));
            row.add(StringUtil.encode("", service.getCharset().getResults()));
            row.add(StringUtil.encode("", service.getCharset().getResults()));
            row.add(StringUtil.encode("", service.getCharset().getResults()));
            row.setPacketId(++packetId);
            buffer = row.write(buffer, service, true);
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
