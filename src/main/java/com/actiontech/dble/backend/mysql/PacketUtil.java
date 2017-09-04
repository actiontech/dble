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
package com.actiontech.dble.backend.mysql;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResultSetHeaderPacket;

import java.io.UnsupportedEncodingException;

/**
 * @author mycat
 */
public final class PacketUtil {
    private PacketUtil() {
    }

    private static final String UTF8 = "utf8";

    public static ResultSetHeaderPacket getHeader(int fieldCount) {
        ResultSetHeaderPacket packet = new ResultSetHeaderPacket();
        packet.setPacketId(1);
        packet.setFieldCount(fieldCount);
        return packet;
    }

    private static byte[] encode(String src, String charset) {
        if (src == null) {
            return null;
        }
        try {
            return src.getBytes(CharsetUtil.getJavaCharset(charset));
        } catch (UnsupportedEncodingException e) {
            return src.getBytes();
        }
    }

    public static FieldPacket getField(String name, String orgName, int type) {
        FieldPacket packet = new FieldPacket();
        packet.setCharsetIndex(CharsetUtil.getIndex(UTF8));
        packet.setName(encode(name, UTF8));
        packet.setOrgName(encode(orgName, UTF8));
        packet.setType(type);
        return packet;
    }

    public static FieldPacket getField(String name, int type) {
        FieldPacket packet = new FieldPacket();
        packet.setCharsetIndex(CharsetUtil.getIndex(UTF8));
        packet.setName(encode(name, UTF8));
        packet.setType(type);
        return packet;
    }

    public static ErrorPacket getShutdown() {
        ErrorPacket error = new ErrorPacket();
        error.setPacketId(1);
        error.setErrno(ErrorCode.ER_SERVER_SHUTDOWN);
        error.setMessage("The server has been shutdown".getBytes());
        return error;
    }

}
