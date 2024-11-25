/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.util;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.concurrent.ConcurrentHashMap;

public final class Security {

    private static final ConcurrentHashMap<String, Charset> CHARSET = new ConcurrentHashMap<>();

    /**
     * Prevent construction.
     */
    private Security() {
        super();
    }

    /**
     * Encrypt/Decrypt function used for password encryption in authentication
     * Simple XOR is used here but it is OK as we encrypt random strings
     *
     * @param from     IN Data for encryption
     * @param to       OUT Encrypt data to the buffer (may be the same)
     * @param scramble IN Scramble used for encryption
     * @param length   IN Length of data to encrypt
     */
    public static void xorString(byte[] from, byte[] to, byte[] scramble, int length) {
        int pos = 0;
        int scrambleLength = scramble.length;

        while (pos < length) {
            to[pos] = (byte) (from[pos] ^ scramble[pos % scrambleLength]);
            pos++;
        }
    }

    public static byte[] getBytes(char[] value, String encoding) throws UnsupportedEncodingException {
        return getBytes(value, 0, value.length, encoding);
    }


    public static byte[] getBytes(char[] value, int offset, int length, String encoding) throws UnsupportedEncodingException {
        Charset cs = findCharset(encoding);

        ByteBuffer buf = cs.encode(CharBuffer.wrap(value, offset, length));

        // can't simply .array() this to get the bytes especially with variable-length charsets the buffer is sometimes larger than the actual encoded data
        int encodedLen = buf.limit();
        byte[] asBytes = new byte[encodedLen];
        buf.get(asBytes, 0, encodedLen);

        return asBytes;
    }

    static Charset findCharset(String alias) throws UnsupportedEncodingException {
        try {
            Charset cs = CHARSET.get(alias);

            if (cs == null) {
                cs = Charset.forName(alias);
                Charset oldCs = CHARSET.putIfAbsent(alias, cs);
                if (oldCs != null) {
                    // if the previous value was recently set by another thread we return it instead of value we found here
                    cs = oldCs;
                }
            }

            return cs;

            // We re-throw these runtimes for compatibility with java.io
        } catch (IllegalArgumentException uce) {
            throw new UnsupportedEncodingException(alias);
        }
    }

}
