/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server.response;

import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResultSetHeaderPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.server.ServerConnection;
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


    public static void execute(ServerConnection c, String sql) {
        String subSql = sql.substring(sql.indexOf("SELECT") + 6);
        List<String> splitVar = Splitter.on(",").omitEmptyStrings().trimResults().splitToList(subSql);
        splitVar = convert(splitVar);
        int fieldCount = splitVar.size();
        ResultSetHeaderPacket header = PacketUtil.getHeader(fieldCount);
        FieldPacket[] fields = new FieldPacket[fieldCount];

        int i = 0;
        byte packetId = 0;
        header.setPacketId(++packetId);
        for (String s : splitVar) {
            fields[i] = PacketUtil.getField(s, Fields.FIELD_TYPE_VAR_STRING);
            fields[i++].setPacketId(++packetId);
        }


        ByteBuffer buffer = c.allocate();

        // write header
        buffer = header.write(buffer, c, true);

        // write fields
        for (FieldPacket field : fields) {
            buffer = field.write(buffer, c, true);
        }


        EOFPacket eof = new EOFPacket();
        eof.setPacketId(++packetId);
        // write eof
        buffer = eof.write(buffer, c, true);

        // write rows
        //byte packetId = eof.packetId;

        RowDataPacket row = new RowDataPacket(fieldCount);
        for (String s : splitVar) {
            String value = VARIABLES.get(s) == null ? "" : VARIABLES.get(s);
            row.add(value.getBytes());

        }

        row.setPacketId(++packetId);
        buffer = row.write(buffer, c, true);


        // write lastEof
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, c, true);

        // write buffer
        c.write(buffer);
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
        VARIABLES.put("@@character_set_client", "utf8");
        VARIABLES.put("@@character_set_connection", "utf8");
        VARIABLES.put("@@character_set_results", "utf8");
        VARIABLES.put("@@character_set_server", "utf8");
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
        VARIABLES.put("@@tx_isolation", "REPEATABLE-READ");
        VARIABLES.put("@@wait_timeout", "172800");
        VARIABLES.put("@@session.auto_increment_increment", "1");

        VARIABLES.put("character_set_client", "utf8");
        VARIABLES.put("character_set_connection", "utf8");
        VARIABLES.put("character_set_results", "utf8");
        VARIABLES.put("character_set_server", "utf8");
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
        VARIABLES.put("tx_isolation", "REPEATABLE-READ");
        VARIABLES.put("wait_timeout", "172800");
        VARIABLES.put("auto_increment_increment", "1");
    }


}
