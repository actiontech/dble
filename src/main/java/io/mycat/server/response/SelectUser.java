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
package io.mycat.server.response;

import io.mycat.MycatServer;
import io.mycat.backend.mysql.PacketUtil;
import io.mycat.config.Fields;
import io.mycat.net.mysql.*;
import io.mycat.server.ServerConnection;
import io.mycat.util.StringUtil;

import java.nio.ByteBuffer;

/**
 * @author mycat
 */
public final class SelectUser {
    private SelectUser() {
    }

    private static final int FIELD_COUNT = 1;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();
    private static final ErrorPacket ERROR = PacketUtil.getShutdown();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.packetId = ++packetId;
        FIELDS[i] = PacketUtil.getField("USER()", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i].packetId = ++packetId;
        EOF.packetId = ++packetId;
    }

    public static void response(ServerConnection c) {
        if (MycatServer.getInstance().isOnline()) {
            ByteBuffer buffer = c.allocate();
            buffer = HEADER.write(buffer, c, true);
            for (FieldPacket field : FIELDS) {
                buffer = field.write(buffer, c, true);
            }
            buffer = EOF.write(buffer, c, true);
            byte packetId = EOF.packetId;
            RowDataPacket row = new RowDataPacket(FIELD_COUNT);
            row.add(getUser(c));
            row.packetId = ++packetId;
            buffer = row.write(buffer, c, true);
            EOFPacket lastEof = new EOFPacket();
            lastEof.packetId = ++packetId;
            buffer = lastEof.write(buffer, c, true);
            c.write(buffer);
        } else {
            ERROR.write(c);
        }
    }

    private static byte[] getUser(ServerConnection c) {
        StringBuilder sb = new StringBuilder();
        sb.append(c.getUser()).append('@').append(c.getHost());
        return StringUtil.encode(sb.toString(), c.getCharset());
    }

}
