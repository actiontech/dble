/*
* Copyright (C) 2016-2020 ActionTech.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ConcurrentHashMap;

public final class Security {
    private static final char PVERSION41_CHAR = '*';

    private static final Logger LOGGER = LoggerFactory.getLogger(Security.class);
    private static final int SHA1_HASH_SIZE = 20;

    private static int cachingSha2DigestLength = 32;

    /**
     * Returns hex value for given char
     */
    private static int charVal(char c) {
        return ((c >= '0') && (c <= '9')) ? (c - '0') : (((c >= 'A') && (c <= 'Z')) ? (c - 'A' + 10) : (c - 'a' + 10));
    }

    /*
     * Convert password in salted form to binary string password and hash-salt
     * For old password this involves one more hashing
     * SYNOPSIS get_hash_and_password() salt IN Salt to convert from pversion IN
     * Password version to use hash OUT Store zero ended hash here bin_password
     * OUT Store binary password here (no zero at the end)
     * RETURN 0 for pre 4.1 passwords !0 password version char for newer
     * passwords
     */

    /**
     * Creates key from old password to decode scramble Used in 4.1
     * authentication with passwords stored pre-4.1 hashing.
     *
     * @param passwd the password to create the key from
     * @return 20 byte generated key
     * @throws NoSuchAlgorithmException if the message digest 'SHA-1' is not available.
     */
    static byte[] createKeyFromOldPassword(String passwd) throws NoSuchAlgorithmException {
        /* At first hash password to the string stored in password */
        passwd = makeScrambledPassword(passwd);

        /* Now convert it to the salt form */
        int[] salt = getSaltFromPassword(passwd);

        /* Finally get hash and bin password from salt */
        return getBinaryPassword(salt, false);
    }

    /**
     * @param salt
     * @param usingNewPasswords
     * @throws NoSuchAlgorithmException if the message digest 'SHA-1' is not available.
     */
    static byte[] getBinaryPassword(int[] salt, boolean usingNewPasswords) throws NoSuchAlgorithmException {
        int val = 0;

        byte[] binaryPassword = new byte[SHA1_HASH_SIZE]; /* Binary password loop pointer */

        if (usingNewPasswords) /* New password version assumed */ {
            int pos = 0;

            for (int i = 0; i < 4; i++) /* Iterate over these elements */ {
                val = salt[i];

                for (int t = 3; t >= 0; t--) {
                    binaryPassword[pos++] = (byte) (val & 255);
                    val >>= 8; /* Scroll 8 bits to get next part */
                }
            }

            return binaryPassword;
        }

        int offset = 0;

        for (int i = 0; i < 2; i++) /* Iterate over these elements */ {
            val = salt[i];

            for (int t = 3; t >= 0; t--) {
                binaryPassword[t + offset] = (byte) (val % 256);
                val >>= 8; /* Scroll 8 bits to get next part */
            }

            offset += 4;
        }

        MessageDigest md = MessageDigest.getInstance("SHA-1");

        md.update(binaryPassword, 0, 8);

        return md.digest();
    }

    private static int[] getSaltFromPassword(String password) {
        int[] result = new int[6];

        if ((password == null) || (password.length() == 0)) {
            return result;
        }

        if (password.charAt(0) == PVERSION41_CHAR) {
            // new password
            String saltInHex = password.substring(1, 5);

            int val = 0;

            for (int i = 0; i < 4; i++) {
                val = (val << 4) + charVal(saltInHex.charAt(i));
            }

            return result;
        }

        int resultPos = 0;
        int pos = 0;
        int length = password.length();

        while (pos < length) {
            int val = 0;

            for (int i = 0; i < 8; i++) {
                val = (val << 4) + charVal(password.charAt(pos++));
            }

            result[resultPos++] = val;
        }

        return result;
    }

    private static String longToHex(long val) {
        String longHex = Long.toHexString(val);

        int length = longHex.length();

        if (length < 8) {
            int padding = 8 - length;
            StringBuilder buf = new StringBuilder();

            for (int i = 0; i < padding; i++) {
                buf.append("0");
            }

            buf.append(longHex);

            return buf.toString();
        }

        return longHex.substring(0, 8);
    }

    /**
     * Creates password to be stored in user database from raw string.
     * Handles Pre-MySQL 4.1 passwords.
     *
     * @param password plaintext password
     * @return scrambled password
     * @throws NoSuchAlgorithmException if the message digest 'SHA-1' is not available.
     */
    static String makeScrambledPassword(String password) throws NoSuchAlgorithmException {
        long[] passwordHash = hashPre41Password(password);
        StringBuilder scramble = new StringBuilder();

        scramble.append(longToHex(passwordHash[0]));
        scramble.append(longToHex(passwordHash[1]));

        return scramble.toString();
    }

    public static long[] hashPre41Password(String password) {
        return hashPre41Password(password, Charset.defaultCharset().name());
    }

    public static long[] hashPre41Password(String password, String encoding) {
        // remove white spaces and convert to bytes
        try {
            return newHash(password.replaceAll("\\s", "").getBytes(encoding));
        } catch (UnsupportedEncodingException e) {
            return new long[0];
        }
    }


    static long[] newHash(byte[] password) {
        long nr = 1345345333L;
        long add = 7;
        long nr2 = 0x12345671L;
        long tmp;

        for (byte b : password) {
            tmp = 0xff & b;
            nr ^= ((((nr & 63) + add) * tmp) + (nr << 8));
            nr2 += ((nr2 << 8) ^ nr);
            add += tmp;
        }

        long[] result = new long[2];
        result[0] = nr & 0x7fffffffL;
        result[1] = nr2 & 0x7fffffffL;

        return result;
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

    /**
     * Stage one password hashing, used in MySQL 4.1 password handling
     * @param value password
     * @param encoding encode
     * @return stage one hash of password
     * @throws NoSuchAlgorithmException if the message digest 'SHA-1' is not available.
     */

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
            Charset cs = CHARSETSBYALIAS.get(alias);

            if (cs == null) {
                cs = Charset.forName(alias);
                Charset oldCs = CHARSETSBYALIAS.putIfAbsent(alias, cs);
                if (oldCs != null) {
                    // if the previous value was recently set by another thread we return it instead of value we found here
                    cs = oldCs;
                }
            }

            return cs;

            // We re-throw these runtimes for compatibility with java.io
        } catch (UnsupportedCharsetException uce) {
            throw new UnsupportedEncodingException(alias);
        } catch (IllegalCharsetNameException icne) {
            throw new UnsupportedEncodingException(alias);
        } catch (IllegalArgumentException iae) {
            throw new UnsupportedEncodingException(alias);
        }
    }

    private static final ConcurrentHashMap<String, Charset> CHARSETSBYALIAS = new ConcurrentHashMap<String, Charset>();


    /**
     * Stage two password hashing used in MySQL 4.1 password handling
     *
     * @param salt salt used for stage two hashing
     * @return result of stage two password hash
     * @throws NoSuchAlgorithmException if the message digest 'SHA-1' is not available.
     * @paramhash from passwordHashStage1
     */
    static byte[] passwordHashStage2(byte[] hashedPassword, byte[] salt) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-1");

        // hash 4 bytes of salt
        md.update(salt, 0, 4);

        md.update(hashedPassword, 0, SHA1_HASH_SIZE);

        return md.digest();
    }

    /**
     * Scrambling for caching_sha2_password plugin.
     * <pre>
     * Scramble = XOR(SHA2(password), SHA2(SHA2(SHA2(password)), Nonce))
     * </pre>
     *
     * @throws DigestException
     */
    public static byte[] scrambleCachingSha2(byte[] password, byte[] seed) throws DigestException {
        MessageDigest md;
        byte[] mysqlScrambleBuff = null;
        try {
            md = MessageDigest.getInstance("SHA-256");
            byte[] dig1 = new byte[cachingSha2DigestLength];
            byte[] dig2 = new byte[cachingSha2DigestLength];
            byte[] scramble1 = new byte[cachingSha2DigestLength];

            // SHA2(src) => digest_stage1
            md.update(password, 0, password.length);
            md.digest(dig1, 0, cachingSha2DigestLength);
            md.reset();

            // SHA2(digest_stage1) => digest_stage2
            md.update(dig1, 0, dig1.length);
            md.digest(dig2, 0, cachingSha2DigestLength);
            md.reset();

            // SHA2(digest_stage2, m_rnd) => scramble_stage1
            md.update(dig2, 0, dig1.length);
            md.update(seed, 0, seed.length);
            md.digest(scramble1, 0, cachingSha2DigestLength);

            // XOR(digest_stage1, scramble_stage1) => scramble
            mysqlScrambleBuff = new byte[cachingSha2DigestLength];
            xorString(dig1, mysqlScrambleBuff, scramble1, cachingSha2DigestLength);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return mysqlScrambleBuff;
    }

    /**
     * Prevent construction.
     */
    private Security() {
        super();
    }

}
