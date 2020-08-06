/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.services.mysqlauthenticate;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * SecurityUtil
 *
 * @author mycat
 */
public final class SecurityUtil {
    private SecurityUtil() {
    }

    public static byte[] scramble256(byte[] pass, byte[] seed) throws NoSuchAlgorithmException {
        try {
            int cachingSha2DigestLength = 32;
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] dig1 = new byte[cachingSha2DigestLength];
            byte[] dig2 = new byte[cachingSha2DigestLength];
            md.update(pass, 0, pass.length);
            md.digest(dig1, 0, cachingSha2DigestLength);
            md.reset();
            md.update(dig1, 0, dig1.length);
            md.digest(dig2, 0, cachingSha2DigestLength);
            md.reset();

            md.update(dig2, 0, dig1.length);
            md.update(seed, 0, seed.length);
            byte[] scramble1 = new byte[cachingSha2DigestLength];
            md.digest(scramble1, 0, cachingSha2DigestLength);

            byte[] mysqlScrambleBuff = new byte[cachingSha2DigestLength];
            PasswordAuthPlugin.xorString(dig1, mysqlScrambleBuff, scramble1, cachingSha2DigestLength);

            return mysqlScrambleBuff;
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return pass;
    }

    public static byte[] scramble411(byte[] pass, byte[] seed) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-1");
        byte[] pass1 = md.digest(pass);
        md.reset();
        byte[] pass2 = md.digest(pass1);
        md.reset();
        md.update(seed);
        byte[] pass3 = md.digest(pass2);
        for (int i = 0; i < pass3.length; i++) {
            pass3[i] = (byte) (pass3[i] ^ pass1[i]);
        }
        return pass3;
    }

    public static String scramble323(String pass, String seed) {
        if ((pass == null) || (pass.length() == 0)) {
            return pass;
        }
        byte b;
        double d;
        long[] pw = hash(seed);
        long[] msg = hash(pass);
        long max = 0x3fffffffL;
        long seed1 = (pw[0] ^ msg[0]) % max;
        long seed2 = (pw[1] ^ msg[1]) % max;
        char[] chars = new char[seed.length()];
        for (int i = 0; i < seed.length(); i++) {
            seed1 = ((seed1 * 3) + seed2) % max;
            seed2 = (seed1 + seed2 + 33) % max;
            d = (double) seed1 / (double) max;
            b = (byte) Math.floor((d * 31) + 64);
            chars[i] = (char) b;
        }
        seed1 = ((seed1 * 3) + seed2) % max;
        //seed2 = (seed1 + seed2 + 33) % max;
        d = (double) seed1 / (double) max;
        b = (byte) Math.floor(d * 31);
        for (int i = 0; i < seed.length(); i++) {
            chars[i] ^= (char) b;
        }
        return new String(chars);
    }

    private static long[] hash(String src) {
        long nr = 1345345333L;
        long add = 7;
        long nr2 = 0x12345671L;
        long tmp;
        for (int i = 0; i < src.length(); ++i) {
            switch (src.charAt(i)) {
                case ' ':
                case '\t':
                    continue;
                default:
                    tmp = (0xff & src.charAt(i));
                    nr ^= ((((nr & 63) + add) * tmp) + (nr << 8));
                    nr2 += ((nr2 << 8) ^ nr);
                    add += tmp;
            }
        }
        long[] result = new long[2];
        result[0] = nr & 0x7fffffffL;
        result[1] = nr2 & 0x7fffffffL;
        return result;
    }

}
