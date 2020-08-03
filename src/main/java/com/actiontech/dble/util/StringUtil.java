/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.util;

import com.actiontech.dble.backend.mysql.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * @author mycat
 */
public final class StringUtil {
    private StringUtil() {
    }

    public static final String TABLE_COLUMN_SEPARATOR = ".";

    private static final Logger LOGGER = LoggerFactory.getLogger(StringUtil.class);
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    private static final Random RANDOM = new Random();
    private static final char[] CHARS = {'1', '2', '3', '4', '5', '6', '7',
            '8', '9', '0', 'q', 'w', 'e', 'r', 't', 'y', 'u', 'i', 'o', 'p',
            'a', 's', 'd', 'f', 'g', 'h', 'j', 'k', 'l', 'z', 'x', 'c', 'v',
            'b', 'n', 'm', 'Q', 'W', 'E', 'R', 'T', 'Y', 'U', 'I', 'O', 'P',
            'A', 'S', 'D', 'F', 'G', 'H', 'J', 'K', 'L', 'Z', 'X', 'C', 'V',
            'B', 'N', 'M'};

    /**
     * String hash:s[0]*31^(n-1) + s[1]*31^(n-2) + ... + s[n-1] <br>
     * h = 31*h + s.charAt(i); => h = (h << 5) - h + s.charAt(i); <br>
     *
     * @param start hash for s.substring(start, end)
     * @param end   hash for s.substring(start, end)
     */
    public static long hash(String s, int start, int end) {
        if (start < 0) {
            start = 0;
        }
        if (end > s.length()) {
            end = s.length();
        }
        long h = 0;
        for (int i = start; i < end; ++i) {
            h = (h << 5) - h + s.charAt(i);
        }
        return h;
    }

    public static byte[] encode(String src, String charset) {
        if (src == null) {
            return null;
        }
        if (charset == null) {
            return src.getBytes();
        }
        try {
            return src.getBytes(CharsetUtil.getJavaCharset(charset));
        } catch (UnsupportedEncodingException e) {
            return src.getBytes();
        }
    }

    public static String decode(byte[] src, String charset) {
        return decode(src, 0, src.length, charset);
    }

    public static String decode(byte[] src, int offset, int length,
                                String charset) {
        try {
            return new String(src, offset, length, CharsetUtil.getJavaCharset(charset));
        } catch (UnsupportedEncodingException e) {
            return new String(src, offset, length);
        }
    }

    public static String getRandomString(int size) {
        StringBuilder s = new StringBuilder(size);
        int len = CHARS.length;
        for (int i = 0; i < size; i++) {
            int x = RANDOM.nextInt();
            s.append(CHARS[(x < 0 ? -x : x) % len]);
        }
        return s.toString();
    }


    public static boolean isEmpty(String str) {
        return ((str == null) || (str.length() == 0));
    }

    public static byte[] hexString2Bytes(char[] hexString, int offset,
                                         int length) {
        if (hexString == null) {
            return null;
        }
        if (length == 0) {
            return EMPTY_BYTE_ARRAY;
        }
        boolean odd = length << 31 == Integer.MIN_VALUE;
        byte[] bs = new byte[odd ? (length + 1) >> 1 : length >> 1];
        for (int i = offset, limit = offset + length; i < limit; ++i) {
            char high, low;
            if (i == offset && odd) {
                high = '0';
                low = hexString[i];
            } else {
                high = hexString[i];
                low = hexString[++i];
            }
            int b;
            switch (high) {
                case '0':
                    b = 0;
                    break;
                case '1':
                    b = 0x10;
                    break;
                case '2':
                    b = 0x20;
                    break;
                case '3':
                    b = 0x30;
                    break;
                case '4':
                    b = 0x40;
                    break;
                case '5':
                    b = 0x50;
                    break;
                case '6':
                    b = 0x60;
                    break;
                case '7':
                    b = 0x70;
                    break;
                case '8':
                    b = 0x80;
                    break;
                case '9':
                    b = 0x90;
                    break;
                case 'a':
                case 'A':
                    b = 0xa0;
                    break;
                case 'b':
                case 'B':
                    b = 0xb0;
                    break;
                case 'c':
                case 'C':
                    b = 0xc0;
                    break;
                case 'd':
                case 'D':
                    b = 0xd0;
                    break;
                case 'e':
                case 'E':
                    b = 0xe0;
                    break;
                case 'f':
                case 'F':
                    b = 0xf0;
                    break;
                default:
                    throw new IllegalArgumentException("illegal hex-string: " + new String(hexString, offset, length));
            }
            switch (low) {
                case '0':
                    break;
                case '1':
                    b += 1;
                    break;
                case '2':
                    b += 2;
                    break;
                case '3':
                    b += 3;
                    break;
                case '4':
                    b += 4;
                    break;
                case '5':
                    b += 5;
                    break;
                case '6':
                    b += 6;
                    break;
                case '7':
                    b += 7;
                    break;
                case '8':
                    b += 8;
                    break;
                case '9':
                    b += 9;
                    break;
                case 'a':
                case 'A':
                    b += 10;
                    break;
                case 'b':
                case 'B':
                    b += 11;
                    break;
                case 'c':
                case 'C':
                    b += 12;
                    break;
                case 'd':
                case 'D':
                    b += 13;
                    break;
                case 'e':
                case 'E':
                    b += 14;
                    break;
                case 'f':
                case 'F':
                    b += 15;
                    break;
                default:
                    throw new IllegalArgumentException("illegal hex-string: " + new String(hexString, offset, length));
            }
            bs[(i - offset) >> 1] = (byte) b;
        }
        return bs;
    }

    public static String dumpAsHex(byte[] src, int length) {
        StringBuilder out = new StringBuilder(length * 4);
        int p = 0;
        int rows = length / 8;
        for (int i = 0; (i < rows) && (p < length); i++) {
            int ptemp = p;
            for (int j = 0; j < 8; j++) {
                String hexVal = Integer.toHexString(src[ptemp] & 0xff);
                if (hexVal.length() == 1) {
                    out.append('0');
                }
                out.append(hexVal).append(' ');
                ptemp++;
            }
            out.append("    ");
            for (int j = 0; j < 8; j++) {
                int b = 0xff & src[p];
                if (b > 32 && b < 127) {
                    out.append((char) b).append(' ');
                } else {
                    out.append(". ");
                }
                p++;
            }
            out.append('\n');
        }
        int n = 0;
        for (int i = p; i < length; i++) {
            String hexVal = Integer.toHexString(src[i] & 0xff);
            if (hexVal.length() == 1) {
                out.append('0');
            }
            out.append(hexVal).append(' ');
            n++;
        }
        for (int i = n; i < 8; i++) {
            out.append("   ");
        }
        out.append("    ");
        for (int i = p; i < length; i++) {
            int b = 0xff & src[i];
            if (b > 32 && b < 127) {
                out.append((char) b).append(' ');
            } else {
                out.append(". ");
            }
        }
        out.append('\n');
        return out.toString();
    }

    public static byte[] escapeEasternUnicodeByteStream(byte[] src,
                                                        String srcString, int offset, int length) {
        if ((src == null) || (src.length == 0)) {
            return src;
        }
        int bytesLen = src.length;
        int bufIndex = 0;
        int strIndex = 0;
        ByteArrayOutputStream out = new ByteArrayOutputStream(bytesLen);
        while (true) {
            if (srcString.charAt(strIndex) == '\\') { // write it out as-is
                out.write(src[bufIndex++]);
            } else { // Grab the first byte
                int loByte = src[bufIndex];
                if (loByte < 0) {
                    loByte += 256; // adjust for signedness/wrap-around
                }
                out.write(loByte); // We always write the first byte
                if (loByte >= 0x80) {
                    if (bufIndex < (bytesLen - 1)) {
                        int hiByte = src[bufIndex + 1];
                        if (hiByte < 0) {
                            hiByte += 256; // adjust for signedness/wrap-around
                        }
                        out.write(hiByte); // write the high byte here, and
                        // increment the index for the high
                        // byte
                        bufIndex++;
                        if (hiByte == 0x5C) {
                            out.write(hiByte); // escape 0x5c if necessary
                        }
                    }
                } else if (loByte == 0x5c && bufIndex < (bytesLen - 1)) {
                    int hiByte = src[bufIndex + 1];
                    if (hiByte < 0) {
                        hiByte += 256; // adjust for signedness/wrap-around
                    }
                    if (hiByte == 0x62) { // we need to escape the 0x5c
                        out.write(0x5c);
                        out.write(0x62);
                        bufIndex++;
                    }
                }
                bufIndex++;
            }
            if (bufIndex >= bytesLen) {
                break; // we're done
            }
            strIndex++;
        }
        return out.toByteArray();
    }

    public static String toString(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return "";
        }
        StringBuilder buffer = new StringBuilder();
        for (byte byt : bytes) {
            buffer.append((char) byt);
        }
        return buffer.toString();
    }

    public static boolean equalsIgnoreCase(String str1, String str2) {
        if (str1 == null) {
            return str2 == null;
        }
        return str1.equalsIgnoreCase(str2);
    }

    public static boolean equals(String str1, String str2) {
        if (str1 == null) {
            return str2 == null;
        }
        return str1.equals(str2);
    }

    public static boolean equalsWithEmpty(String str1, String str2) {
        if (isEmpty(str1)) {
            return isEmpty(str2);
        }
        return str1.equals(str2);
    }

    public static int countChar(String str, char c) {
        if (str == null || str.isEmpty()) {
            return 0;
        }
        final int len = str.length();
        int cnt = 0;
        for (int i = 0; i < len; ++i) {
            if (c == str.charAt(i)) {
                ++cnt;
            }
        }
        return cnt;
    }

    public static String replaceOnce(String text, String repl, String with) {
        return replace(text, repl, with, 1);
    }

    public static String replace(String text, String repl, String with) {
        return replace(text, repl, with, -1);
    }

    public static String replace(String text, String repl, String with, int max) {
        if ((text == null) || (repl == null) || (with == null) || (repl.length() == 0) || (max == 0)) {
            return text;
        }
        StringBuilder buf = new StringBuilder(text.length());
        int start = 0;
        int end = 0;
        while ((end = text.indexOf(repl, start)) != -1) {
            buf.append(text.substring(start, end)).append(with);
            start = end + repl.length();
            if (--max == 0) {
                break;
            }
        }
        buf.append(text.substring(start));
        return buf.toString();
    }

    public static String replaceChars(String str, char searchChar,
                                      char replaceChar) {
        if (str == null) {
            return null;
        }
        return str.replace(searchChar, replaceChar);
    }

    public static String replaceChars(String str, String searchChars,
                                      String replaceChars) {
        if ((str == null) || (str.length() == 0) || (searchChars == null) || (searchChars.length() == 0)) {
            return str;
        }
        char[] chars = str.toCharArray();
        int len = chars.length;
        boolean modified = false;
        for (int i = 0, isize = searchChars.length(); i < isize; i++) {
            char searchChar = searchChars.charAt(i);
            if ((replaceChars == null) || (i >= replaceChars.length())) { // DELETE
                int pos = 0;
                for (int j = 0; j < len; j++) {
                    if (chars[j] != searchChar) {
                        chars[pos++] = chars[j];
                    } else {
                        modified = true;
                    }
                }
                len = pos;
            } else { // REPLACE
                for (int j = 0; j < len; j++) {
                    if (chars[j] == searchChar) {
                        chars[j] = replaceChars.charAt(i);
                        modified = true;
                    }
                }
            }
        }
        if (!modified) {
            return str;
        }
        return new String(chars, 0, len);
    }

    /**
     * remove ` in `tablename`
     * FIXME```tablename` may not correct
     *
     * @param str
     * @return
     */
    public static String removeBackQuote(String str) {
        if (str.length() > 1) {
            char firstValue = str.charAt(0);
            if ((firstValue == '`') && (firstValue == str.charAt(str.length() - 1))) {
                return str.substring(1, str.length() - 1);
            } else {
                return str;
            }
        }
        return str;
    }

    /**
     * remove ' from 'value'
     *
     * @param str
     * @return
     */
    public static String removeApostrophe(String str) {
        if (str.length() > 1) {
            char firstValue = str.charAt(0);
            if ((firstValue == '\'') && (firstValue == str.charAt(str.length() - 1))) {
                return str.substring(1, str.length() - 1);
            } else {
                return str;
            }
        }
        return str;
    }

    public static boolean containsApostrophe(String tableName) {
        char firstValue = tableName.charAt(0);
        return (firstValue == '`') && (firstValue == tableName.charAt(tableName.length() - 1));
    }

    public static String removeAllApostrophe(String str) {
        if (str.length() > 1) {
            char firstValue = str.charAt(0);
            if ((firstValue == '\'' | firstValue == '"') && (firstValue == str.charAt(str.length() - 1))) {
                return str.substring(1, str.length() - 1);
            } else {
                return str;
            }
        }
        return str;
    }

    /**
     * remove ' from 'value'
     *
     * @param str
     * @return
     */
    public static String removeApostropheOrBackQuote(String str) {
        if (str.length() > 1) {
            char firstValue = str.charAt(0);
            if (((firstValue == '\'') || (firstValue == '`')) && (firstValue == str.charAt(str.length() - 1))) {
                return str.substring(1, str.length() - 1);
            } else {
                return str;
            }
        }
        return str;
    }

    public static String getFullName(String schema, String tableName, char split) {
        return String.format("`%s`" + split + "`%s`", schema, tableName);
    }

    public static String getFullName(String schema, String tableName) {
        return String.format("`%s`.`%s`", schema, tableName);
    }


    public static String getUFullName(String schema, String tableName) {
        return String.format("%s.%s", schema, tableName);
    }

    public static String join(List<String> list, String flag) {
        if (list.size() < 1) {
            return "";
        }

        String[] arr = list.toArray(new String[list.size()]);
        return join(arr, flag);
    }

    public static String join(Set<String> set, String flag) {
        if (set.size() < 1) {
            return "";
        }

        String[] arr = set.toArray(new String[set.size()]);
        return join(arr, flag);
    }

    public static String join(String[] list, String flag) {
        if (list.length < 1) {
            return "";
        }
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < list.length - 1; i++) {
            sb.append(list[i]).append(flag);
        }
        sb.append(list[list.length - 1]);
        return sb.toString();
    }


    public static String trim(String orgStr, char c) {
        if (orgStr != null && orgStr.length() > 1) {
            if (orgStr.charAt(0) == c && orgStr.charAt(orgStr.length() - 1) == c) {
                return orgStr.substring(1, orgStr.length() - 1);
            }
        }
        return orgStr;
    }
}
