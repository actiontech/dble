/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server.handler;

import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.backend.mysql.nio.handler.SingleNodeHandler;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * @author rainbow
 */
public final class Explain2Handler {
    private Explain2Handler() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(Explain2Handler.class);
    private static final RouteResultsetNode[] EMPTY_ARRAY = new RouteResultsetNode[1];
    private static final int FIELD_COUNT = 2;
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];

    static {
        FIELDS[0] = PacketUtil.getField("SQL",
                Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[1] = PacketUtil.getField("MSG",
                Fields.FIELD_TYPE_VAR_STRING);
    }

    public static void handle(String stmt, ShardingService service, int offset) {

        try {
            stmt = stmt.substring(offset);
            if (!stmt.toLowerCase().contains("shardingnode=") || !stmt.toLowerCase().contains("sql=")) {
                showError(stmt, service, "explain2 shardingnode=? sql=?");
                return;
            }
            String shardingNode = stmt.substring(stmt.indexOf("=") + 1, stmt.indexOf("sql=")).trim();
            String sql = "explain " + stmt.substring(stmt.indexOf("sql=") + 4, stmt.length()).trim();

            if (shardingNode.isEmpty() || sql.isEmpty()) {
                showError(stmt, service, "shardingNode or sql is empty");
                return;
            }

            RouteResultsetNode node = new RouteResultsetNode(shardingNode, ServerParse.SELECT, sql);
            RouteResultset rrs = new RouteResultset(sql, ServerParse.SELECT);
            EMPTY_ARRAY[0] = node;
            rrs.setNodes(EMPTY_ARRAY);
            SingleNodeHandler singleNodeHandler = new SingleNodeHandler(rrs, service.getSession2());
            singleNodeHandler.execute();
        } catch (Exception e) {
            LOGGER.info(e.getMessage(), e.getCause());
            showError(stmt, service, e.getMessage());
        }
    }

    private static void showError(String stmt, ShardingService service, String msg) {
        ByteBuffer buffer = service.allocate();
        // writeDirectly header
        ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
        header.setPacketId(service.nextPacketId());
        buffer = header.write(buffer, service, true);

        // writeDirectly fields
        for (FieldPacket field : FIELDS) {
            field.setPacketId(service.nextPacketId());
            buffer = field.write(buffer, service, true);
        }

        // writeDirectly eof
        EOFPacket eof = new EOFPacket();
        eof.setPacketId(service.nextPacketId());
        buffer = eof.write(buffer, service, true);


        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(stmt, service.getCharset().getResults()));
        row.add(StringUtil.encode(msg, service.getCharset().getResults()));
        row.setPacketId(service.nextPacketId());
        buffer = row.write(buffer, service, true);

        // writeDirectly last eof
        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(service.nextPacketId());
        lastEof.write(buffer, service);
    }
}
