/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese
 * opensource volunteers. you can redistribute it and/or modify it under the
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Any questions about this component can be directed to it's project Web address
 * https://code.google.com/p/opencloudb/.
 *
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
