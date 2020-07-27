/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.util;

import java.util.Arrays;

/**
 * @author mycat
 */
public final class HexFormatUtil {
    private HexFormatUtil() {
    }

    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

    public static String bytesToHexString(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
    }

    public static byte[] fromHex(String src) {
        if (src.length() % 2 != 0) {
            src = "0" + src;
        }
        char[] chars = src.toCharArray();
        byte[] b = new byte[chars.length / 2];
        for (int i = 0; i < chars.length; i = i + 2) {
            String tmp = new String(Arrays.copyOfRange(chars, i, i + 2));
            b[i / 2] = (byte) (Integer.parseInt(tmp, 16) & 0xff);
        }
        return b;
    }

    public static String fromHex(String hexString, String charset) {
        if (hexString.equals("\0")) {
            return "NULL";
        }
        try {
            byte[] b = fromHex(hexString);
            if (charset == null) {
                return new String(b);
            }
            return new String(b, charset);
        } catch (Exception e) {
            return null;
        }
    }

    public static int fromHex2B(String src) {
        byte[] b = fromHex(src);
        int position = 0;
        int i = (b[position++] & 0xff);
        i |= (b[position++] & 0xff) << 8;
        return i;
    }

    public static int fromHex4B(String src) {
        byte[] b = fromHex(src);
        int position = 0;
        int i = (b[position++] & 0xff);
        i |= (b[position++] & 0xff) << 8;
        i |= (b[position++] & 0xff) << 16;
        i |= (b[position++] & 0xff) << 24;
        return i;
    }

    public static long fromHex8B(String src) {
        byte[] b = fromHex(src);
        int position = 0;
        long l = (b[position++] & 0xff);
        l |= (long) (b[position++] & 0xff) << 8;
        l |= (long) (b[position++] & 0xff) << 16;
        l |= (long) (b[position++] & 0xff) << 24;
        l |= (long) (b[position++] & 0xff) << 32;
        l |= (long) (b[position++] & 0xff) << 40;
        l |= (long) (b[position++] & 0xff) << 48;
        l |= (long) (b[position++] & 0xff) << 56;
        return l;
    }

}
