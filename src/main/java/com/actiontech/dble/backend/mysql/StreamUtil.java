/*
* Copyright (C) 2016-2021 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.backend.mysql;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author mycat
 */
public final class StreamUtil {
    private StreamUtil() {
    }

    private static final long NULL_LENGTH = -1;
    private static final byte[] EMPTY_BYTES = new byte[0];

    public static void read(InputStream in, byte[] b, int offset, int length) throws IOException {
        for (int got = 0; length > 0; ) {
            got = in.read(b, offset, length);
            if (got < 0) {
                throw new EOFException();
            }
            offset += got;
            length -= got;
        }
    }

    public static byte read(InputStream in) throws IOException {
        int got = in.read();
        if (got < 0) {
            throw new EOFException();
        }
        return (byte) (got & 0xff);
    }

    public static byte[] read(byte[] b, int offset, int length) {
        byte[] fixedByte = new byte[length];
        for (int i = 0 ; i < length ; i++) {
            fixedByte[i] = b[offset + i];
        }
        return fixedByte;
    }

    public static int readBackInt(byte[] b, int offset, int length) {
        byte[] fixedByte = new byte[length];
        for (int i = 0 ; i < length ; i++) {
            fixedByte[i] = b[offset + i];
        }
        int i = fixedByte[0] & 0xff;
        i |= (fixedByte[1] & 0xff) << 8;
        i |= (fixedByte[2] & 0xff) << 16;
        return i;
    }

    public static byte[] readKey(InputStream in, byte[] b, int offset, int length) throws IOException {
        byte[] key = new byte[b.length - 4];
        for (int got = 0; length > 0; ) {
            got = in.read(b, offset, length);
            if (got < 0) {
                throw new EOFException();
            }
            offset += got;
            length -= got;
        }
        for (int i = 4 ; i < b.length ; i++) {
            key[i - 4] = b[i];
        }
        return key;
    }



    public static int readUB2(InputStream in) throws IOException {
        byte[] b = new byte[2];
        read(in, b, 0, b.length);
        int i = b[0] & 0xff;
        i |= (b[1] & 0xff) << 8;
        return i;
    }

    public static int readUB3(InputStream in) throws IOException {
        byte[] b = new byte[3];
        read(in, b, 0, b.length);
        int i = b[0] & 0xff;
        i |= (b[1] & 0xff) << 8;
        i |= (b[2] & 0xff) << 16;
        return i;
    }

    public static int readInt(InputStream in) throws IOException {
        byte[] b = new byte[4];
        read(in, b, 0, b.length);
        int i = b[0] & 0xff;
        i |= (b[1] & 0xff) << 8;
        i |= (b[2] & 0xff) << 16;
        i |= (b[3] & 0xff) << 24;
        return i;
    }

    public static float readFloat(InputStream in) throws IOException {
        return Float.intBitsToFloat(readInt(in));
    }

    public static long readUB4(InputStream in) throws IOException {
        byte[] b = new byte[4];
        read(in, b, 0, b.length);
        long l = (long) (b[0] & 0xff);
        l |= (long) (b[1] & 0xff) << 8;
        l |= (long) (b[2] & 0xff) << 16;
        l |= (long) (b[3] & 0xff) << 24;
        return l;
    }

    public static long readLong(InputStream in) throws IOException {
        byte[] b = new byte[8];
        read(in, b, 0, b.length);
        long l = (long) (b[0] & 0xff);
        l |= (long) (b[1] & 0xff) << 8;
        l |= (long) (b[2] & 0xff) << 16;
        l |= (long) (b[3] & 0xff) << 24;
        l |= (long) (b[4] & 0xff) << 32;
        l |= (long) (b[5] & 0xff) << 40;
        l |= (long) (b[6] & 0xff) << 48;
        l |= (long) (b[7] & 0xff) << 56;
        return l;
    }

    public static double readDouble(InputStream in) throws IOException {
        return Double.longBitsToDouble(readLong(in));
    }

    public static byte[] readWithLength(InputStream in) throws IOException {
        int length = (int) readLength(in);
        if (length <= 0) {
            return EMPTY_BYTES;
        }
        byte[] b = new byte[length];
        read(in, b, 0, b.length);
        return b;
    }


    public static void write(OutputStream out, byte[] src) throws IOException {
        out.write(src);
    }

    public static void write(OutputStream out, byte b) throws IOException {
        out.write(b & 0xff);
    }

    public static void writeUB2(OutputStream out, long i) throws IOException {
        byte[] b = new byte[2];
        b[0] = (byte) (i & 0xff);
        b[1] = (byte) (i >>> 8);
        out.write(b);
    }

    public static void writeUB3(OutputStream out, int i) throws IOException {
        byte[] b = new byte[3];
        b[0] = (byte) (i & 0xff);
        b[1] = (byte) (i >>> 8);
        b[2] = (byte) (i >>> 16);
        out.write(b);
    }

    public static void writeInt(OutputStream out, int i) throws IOException {
        byte[] b = new byte[4];
        b[0] = (byte) (i & 0xff);
        b[1] = (byte) (i >>> 8);
        b[2] = (byte) (i >>> 16);
        b[3] = (byte) (i >>> 24);
        out.write(b);
    }

    public static void writeFloat(OutputStream out, float f) throws IOException {
        writeInt(out, Float.floatToIntBits(f));
    }

    public static void writeUB4(OutputStream out, long l) throws IOException {
        byte[] b = new byte[4];
        b[0] = (byte) (l & 0xff);
        b[1] = (byte) (l >>> 8);
        b[2] = (byte) (l >>> 16);
        b[3] = (byte) (l >>> 24);
        out.write(b);
    }

    public static void writeLong(OutputStream out, long l) throws IOException {
        byte[] b = new byte[8];
        b[0] = (byte) (l & 0xff);
        b[1] = (byte) (l >>> 8);
        b[2] = (byte) (l >>> 16);
        b[3] = (byte) (l >>> 24);
        b[4] = (byte) (l >>> 32);
        b[5] = (byte) (l >>> 40);
        b[6] = (byte) (l >>> 48);
        b[7] = (byte) (l >>> 56);
        out.write(b);
    }

    public static void writeDouble(OutputStream out, double d) throws IOException {
        writeLong(out, Double.doubleToLongBits(d));
    }

    public static long readLength(InputStream in) throws IOException {
        int length = in.read();
        if (length < 0) {
            throw new EOFException();
        }
        switch (length) {
            case 251:
                return NULL_LENGTH;
            case 252:
                return readUB2(in);
            case 253:
                return readUB3(in);
            case 254:
                return readLong(in);
            default:
                return length;
        }
    }

    public static void writeLength(OutputStream out, long length) throws IOException {
        if (length < 251) {
            out.write((byte) length);
        } else if (length < 0x10000L) {
            out.write((byte) 252);
            writeUB2(out, (int) length);
        } else if (length < 0x1000000L) {
            out.write((byte) 253);
            writeUB3(out, (int) length);
        } else {
            out.write((byte) 254);
            writeLong(out, length);
        }
    }

    public static void writeWithNull(OutputStream out, byte[] src) throws IOException {
        out.write(src);
        out.write((byte) 0);
    }

    public static void writeWithLength(OutputStream out, byte[] src) throws IOException {
        int length = src.length;
        if (length < 251) {
            out.write((byte) length);
        } else if (length < 0x10000L) {
            out.write((byte) 252);
            writeUB2(out, length);
        } else if (length < 0x1000000L) {
            out.write((byte) 253);
            writeUB3(out, length);
        } else {
            out.write((byte) 254);
            writeLong(out, length);
        }
        out.write(src);
    }


}
