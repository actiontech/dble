/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server.response;

import com.actiontech.dble.backend.mysql.PreparedStatement;
import com.actiontech.dble.net.FrontendConnection;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.PreparedOkPacket;

import java.nio.ByteBuffer;

/**
 * @author mycat
 */
public final class PreparedStmtResponse {
    private PreparedStmtResponse() {
    }

    public static void response(PreparedStatement pstmt, FrontendConnection c) {
        byte packetId = 0;

        // write preparedOk packet
        PreparedOkPacket preparedOk = new PreparedOkPacket();
        preparedOk.setPacketId(++packetId);
        preparedOk.setStatementId(pstmt.getId());
        preparedOk.setColumnsNumber(pstmt.getColumnsNumber());
        preparedOk.setParametersNumber(pstmt.getParametersNumber());
        ByteBuffer buffer = preparedOk.write(c.allocate(), c, true);

        // write parameter field packet
        int parametersNumber = preparedOk.getParametersNumber();
        if (parametersNumber > 0) {
            for (int i = 0; i < parametersNumber; i++) {
                FieldPacket field = new FieldPacket();
                field.setPacketId(++packetId);
                buffer = field.write(buffer, c, true);
            }
            EOFPacket eof = new EOFPacket();
            eof.setPacketId(++packetId);
            buffer = eof.write(buffer, c, true);
        }

        // write column field packet
        int columnsNumber = preparedOk.getColumnsNumber();
        if (columnsNumber > 0) {
            for (int i = 0; i < columnsNumber; i++) {
                FieldPacket field = new FieldPacket();
                field.setPacketId(++packetId);
                buffer = field.write(buffer, c, true);
            }
            EOFPacket eof = new EOFPacket();
            eof.setPacketId(++packetId);
            buffer = eof.write(buffer, c, true);
        }

        // send buffer
        c.write(buffer);
    }

}
