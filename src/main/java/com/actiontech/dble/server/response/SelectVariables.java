/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server.response;

import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.backend.mysql.VersionUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.google.common.base.Splitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author mycat
 */
public final class SelectVariables {
    private SelectVariables() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(SelectVariables.class);


    public static void execute(ShardingService service, String sql) {
        String subSql = sql.substring(sql.indexOf("SELECT") + 6);
        List<String> splitVar = Splitter.on(",").omitEmptyStrings().trimResults().splitToList(subSql);
        splitVar = convert(splitVar);
        int fieldCount = splitVar.size();
        ResultSetHeaderPacket header = PacketUtil.getHeader(fieldCount);
        FieldPacket[] fields = new FieldPacket[fieldCount];

        int i = 0;
        header.setPacketId(service.nextPacketId());
        for (String s : splitVar) {
            fields[i] = PacketUtil.getField(s, Fields.FIELD_TYPE_VAR_STRING);
            fields[i++].setPacketId(service.nextPacketId());
        }


        ByteBuffer buffer = service.allocate();

        // writeDirectly header
        buffer = header.write(buffer, service, true);

        // writeDirectly fields
        for (FieldPacket field : fields) {
            buffer = field.write(buffer, service, true);
        }


        EOFPacket eof = new EOFPacket();
        eof.setPacketId(service.nextPacketId());
        // writeDirectly eof
        buffer = eof.write(buffer, service, true);

        // writeDirectly rows
        //byte packetId = eof.packetId;

        RowDataPacket row = new RowDataPacket(fieldCount);
        for (String s : splitVar) {
            switch (s.toLowerCase()) {
                case "character_set_client":
                    row.add(service.getCharset().getClient() != null ? service.getCharset().getClient().getBytes() : null);
                    break;
                case "character_set_results":
                    row.add(service.getCharset().getResults() != null ? service.getCharset().getResults().getBytes() : null);
                    break;
                case "collation_connection":
                    row.add(service.getCharset().getCollation() != null ? service.getCharset().getCollation().getBytes() : null);
                    break;
                default:
                    String value = VARIABLES.get(s) == null ? "" : VARIABLES.get(s);
                    row.add(value.getBytes());
            }

        }

        row.setPacketId(service.nextPacketId());
        buffer = row.write(buffer, service, true);


        // writeDirectly lastEof
        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(service.nextPacketId());
        lastEof.write(buffer, service);
    }

    private static List<String> convert(List<String> in) {
        List<String> out = new ArrayList<>();
        for (String s : in) {
            int asIndex = s.toUpperCase().indexOf(" AS ");
            if (asIndex != -1) {
                out.add(s.substring(asIndex + 4));
            }
        }
        if (out.isEmpty()) {
            return in;
        } else {
            return out;
        }


    }


    private static final Map<String, String> VARIABLES = new HashMap<>();

    static {
        VARIABLES.put("@@character_set_client", "utf8mb4");
        VARIABLES.put("@@character_set_connection", "utf8mb4");
        VARIABLES.put("@@character_set_results", "utf8mb4");
        VARIABLES.put("@@character_set_server", "utf8mb4");
        VARIABLES.put("@@init_connect", "");
        VARIABLES.put("@@interactive_timeout", "172800");
        VARIABLES.put("@@license", "GPL");
        VARIABLES.put("@@lower_case_table_names", "1");
        VARIABLES.put("@@max_allowed_packet", "16777216");
        VARIABLES.put("@@net_buffer_length", "16384");
        VARIABLES.put("@@net_write_timeout", "60");
        VARIABLES.put("@@query_cache_size", "0");
        VARIABLES.put("@@query_cache_type", "OFF");
        VARIABLES.put("@@sql_mode", "STRICT_TRANS_TABLES");
        VARIABLES.put("@@system_time_zone", "CST");
        VARIABLES.put("@@time_zone", "SYSTEM");
        VARIABLES.put("@@" + VersionUtil.TRANSACTION_ISOLATION, "REPEATABLE-READ");
        VARIABLES.put("@@" + VersionUtil.TX_ISOLATION, "REPEATABLE-READ");
        VARIABLES.put("@@wait_timeout", "172800");
        VARIABLES.put("@@session.auto_increment_increment", "1");

        VARIABLES.put("character_set_client", "utf8mb4");
        VARIABLES.put("character_set_connection", "utf8mb4");
        VARIABLES.put("character_set_results", "utf8mb4");
        VARIABLES.put("character_set_server", "utf8mb4");
        VARIABLES.put("init_connect", "");
        VARIABLES.put("interactive_timeout", "172800");
        VARIABLES.put("license", "GPL");
        VARIABLES.put("lower_case_table_names", "1");
        VARIABLES.put("max_allowed_packet", "16777216");
        VARIABLES.put("net_buffer_length", "16384");
        VARIABLES.put("net_write_timeout", "60");
        VARIABLES.put("query_cache_size", "0");
        VARIABLES.put("query_cache_type", "OFF");
        VARIABLES.put("sql_mode", "STRICT_TRANS_TABLES");
        VARIABLES.put("system_time_zone", "CST");
        VARIABLES.put("time_zone", "SYSTEM");
        VARIABLES.put(VersionUtil.TRANSACTION_ISOLATION, "REPEATABLE-READ");
        VARIABLES.put(VersionUtil.TX_ISOLATION, "REPEATABLE-READ");
        VARIABLES.put("wait_timeout", "172800");
        VARIABLES.put("auto_increment_increment", "1");
    }


    public static byte setCurrentPacket(ShardingService service) {
        byte packetId = (byte) service.getSession2().getPacketId().get();
        return packetId;
    }


}
