/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.backend.mysql;

import java.nio.ByteBuffer;

/**
 * @author mycat
 */
public final class ByteUtil {
    private ByteUtil() {
    }

    public static int readUB2(byte[] data, int offset) {
        int i = data[offset] & 0xff;
        i |= (data[++offset] & 0xff) << 8;
        return i;
    }

    public static int readUB3(byte[] data, int offset) {
        int i = data[offset] & 0xff;
        i |= (data[++offset] & 0xff) << 8;
        i |= (data[++offset] & 0xff) << 16;
        return i;
    }

    public static long readUB4(byte[] data, int offset) {
        long l = data[offset] & 0xff;
        l |= (data[++offset] & 0xff) << 8;
        l |= (data[++offset] & 0xff) << 16;
        l |= ((long) (data[++offset] & 0xff)) << 24;
        return l;
    }

    public static long readLong(byte[] data, int offset) {
        long l = (long) (data[offset] & 0xff);
        l |= (long) (data[++offset] & 0xff) << 8;
        l |= (long) (data[++offset] & 0xff) << 16;
        l |= (long) (data[++offset] & 0xff) << 24;
        l |= (long) (data[++offset] & 0xff) << 32;
        l |= (long) (data[++offset] & 0xff) << 40;
        l |= (long) (data[++offset] & 0xff) << 48;
        l |= (long) (data[++offset] & 0xff) << 56;
        return l;
    }

    public static long readLength(byte[] data, int offset) {
        int length = data[offset++] & 0xff;
        switch (length) {
            case 251:
                return MySQLMessage.NULL_LENGTH;
            case 252:
                return readUB2(data, offset);
            case 253:
                return readUB3(data, offset);
            case 254:
                return readLong(data, offset);
            default:
                return length;
        }
    }

    public static int lengthToZero(byte[] data, int offset) {
        int start = offset;
        for (int i = start; i < data.length; i++) {
            if (data[i] == 0) {
                return (i - start);
            }
        }
        int remaining = data.length - start;
        return remaining > 0 ? remaining : 0;
    }

    public static int decodeLength(byte[] src) {
        int length = src.length;
        if (length < 251) {
            return 1 + length;
        } else if (length < 0x10000L) {
            return 3 + length;
        } else if (length < 0x1000000L) {
            return 4 + length;
        } else {
            return 9 + length;
        }
    }

    public static int decodeLength(long length) {
        if (length < 251) {
            return 1;
        } else if (length < 0x10000L) {
            return 3;
        } else if (length < 0x1000000L) {
            return 4;
        } else {
            return 9;
        }
    }

    public static void writeUB3(byte[] packet, int val, int offset) {
        packet[offset] = (byte) (val & 0xff);
        packet[offset + 1] = (byte) (val >>> 8);
        packet[offset + 2] = (byte) (val >>> 16);
    }

    public static void writeUB3(ByteBuffer buffer, int val, int offset) {
        buffer.put(offset, (byte) (val & 0xff));
        buffer.put(offset + 1, (byte) (val >>> 8));
        buffer.put(offset + 2, (byte) (val >>> 16));
    }

    public static void writeUB3(byte[] packet, int length) {
        writeUB3(packet, length, 0);
    }

    public static void writeUB4(byte[] packet, long l, int offset) {
        packet[offset] = (byte) (l & 0xff);
        packet[offset + 1] = (byte) (l >>> 8);
        packet[offset + 2] = (byte) (l >>> 16);
        packet[offset + 3] = (byte) (l >>> 24);
    }

}
