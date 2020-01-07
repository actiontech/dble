/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
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
        packet.setCharsetIndex(CharsetUtil.getCharsetDefaultIndex(UTF8));
        packet.setName(encode(name, UTF8));
        packet.setOrgName(encode(orgName, UTF8));
        packet.setType(type);
        return packet;
    }

    public static FieldPacket getField(String name, int type) {
        FieldPacket packet = new FieldPacket();
        packet.setCharsetIndex(CharsetUtil.getCharsetDefaultIndex(UTF8));
        packet.setName(encode(name, UTF8));
        packet.setType(type);
        return packet;
    }

    public static ErrorPacket getShutdown() {
        ErrorPacket error = new ErrorPacket();
        error.setPacketId(1);
        error.setErrNo(ErrorCode.ER_SERVER_SHUTDOWN);
        error.setMessage("The server has been shutdown".getBytes());
        return error;
    }

}
