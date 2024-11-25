/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.server.response;

import com.oceanbase.obsharding_d.backend.mysql.PreparedStatement;
import com.oceanbase.obsharding_d.net.mysql.EOFPacket;
import com.oceanbase.obsharding_d.net.mysql.FieldPacket;
import com.oceanbase.obsharding_d.net.mysql.PreparedOkPacket;
import com.oceanbase.obsharding_d.net.service.ResultFlag;
import com.oceanbase.obsharding_d.net.service.WriteFlags;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;

import java.nio.ByteBuffer;

/**
 * @author mycat
 */
public final class PreparedStmtResponse {
    private PreparedStmtResponse() {
    }

    public static void response(PreparedStatement pStmt, ShardingService service) {
        byte packetId = 0;

        // writeDirectly preparedOk packet
        PreparedOkPacket preparedOk = new PreparedOkPacket();
        preparedOk.setPacketId(++packetId);
        preparedOk.setStatementId(pStmt.getId());
        preparedOk.setColumnsNumber(pStmt.getColumnsNumber());
        preparedOk.setParametersNumber(pStmt.getParametersNumber());
        ByteBuffer buffer = preparedOk.write(service.allocate(), service, true);

        // writeDirectly parameter field packet
        int parametersNumber = preparedOk.getParametersNumber();
        if (parametersNumber > 0) {
            for (int i = 0; i < parametersNumber; i++) {
                FieldPacket field = new FieldPacket();
                field.setPacketId(++packetId);
                buffer = field.write(buffer, service, true);
            }
            EOFPacket eof = new EOFPacket();
            eof.setPacketId(++packetId);
            buffer = eof.write(buffer, service, true);
        }

        // writeDirectly column field packet
        int columnsNumber = preparedOk.getColumnsNumber();
        if (columnsNumber > 0) {
            for (int i = 0; i < columnsNumber; i++) {
                FieldPacket field = new FieldPacket();
                field.setPacketId(++packetId);
                buffer = field.write(buffer, service, true);
            }
            EOFPacket eof = new EOFPacket();
            eof.setPacketId(++packetId);
            buffer = eof.write(buffer, service, true);
        }

        // send buffer
        service.writeDirectly(buffer, WriteFlags.QUERY_END, ResultFlag.OTHER);
    }

}
