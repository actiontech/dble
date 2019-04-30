package com.actiontech.dble.util;

import com.actiontech.dble.backend.mysql.SecurityUtil;
import com.actiontech.dble.config.Capabilities;
import com.actiontech.dble.net.mysql.HandshakeV10Packet;

import javax.crypto.Cipher;
import java.security.KeyFactory;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.sql.SQLException;

public class PasswordAuthPlugin {

    public static byte[] seedTotal = null;
    public static byte[] passwd(String pass, HandshakeV10Packet hs) throws NoSuchAlgorithmException {
        if (pass == null || pass.length() == 0) {
            return null;
        }
        byte[] passwd = pass.getBytes();
        int sl1 = hs.getSeed().length;
        int sl2 = hs.getRestOfScrambleBuff().length;
        byte[] seed = new byte[sl1 + sl2];
        System.arraycopy(hs.getSeed(), 0, seed, 0, sl1);
        System.arraycopy(hs.getRestOfScrambleBuff(), 0, seed, sl1, sl2);
        passwd = SecurityUtil.scramble411(passwd, seed);
        return passwd;
    }

    public static byte[] passwdSha256(String pass, HandshakeV10Packet hs) throws NoSuchAlgorithmException {
        if (pass == null || pass.length() == 0) {
            return null;
        }
        MessageDigest md = null;
        int cachingSha2DigestLength = 32;

        byte[] passwd = pass.getBytes();
        try {
            md = MessageDigest.getInstance("SHA-256");
            byte[] dig1 = new byte[cachingSha2DigestLength];
            byte[] dig2 = new byte[cachingSha2DigestLength];
            md.update(passwd, 0, passwd.length);
            md.digest(dig1, 0, cachingSha2DigestLength);
            md.reset();
            md.update(dig1, 0, dig1.length);
            md.digest(dig2, 0, cachingSha2DigestLength);
            md.reset();

            int sl1 = hs.getSeed().length;
            int sl2 = hs.getRestOfScrambleBuff().length;
            byte[] seed = new byte[sl1 + sl2];
            System.arraycopy(hs.getSeed(), 0, seed, 0, sl1);
            System.arraycopy(hs.getRestOfScrambleBuff(), 0, seed, sl1, sl2);
            md.update(dig2, 0, dig1.length);
            md.update(seed, 0, seed.length);
            byte[] scramble1 = new byte[cachingSha2DigestLength];
            md.digest(scramble1, 0, cachingSha2DigestLength);

            byte[] mysqlScrambleBuff = new byte[cachingSha2DigestLength];
            xorString(dig1, mysqlScrambleBuff, scramble1, cachingSha2DigestLength);
            seedTotal = seed;

            return mysqlScrambleBuff;
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return passwd;
    }


    public static void xorString(byte[] from, byte[] to, byte[] scramble, int length) {
        int pos = 0;
        int scrambleLength = scramble.length;

        while (pos < length) {
            to[pos] = (byte) (from[pos] ^ scramble[pos % scrambleLength]);
            pos++;
        }
    }

    public static RSAPublicKey decodeRSAPublicKey(String key) throws SQLException {
        try {
            if (key == null) {
                throw new SQLException("key parameter is null");
            }
            int offset = key.indexOf("\n") + 1;
            int len = key.indexOf("-----END PUBLIC KEY-----") - offset;

            // TODO: use standard decoders with Java 6+
            byte[] certificateData = Base64Decoder.decode(key.getBytes(), offset, len);
            X509EncodedKeySpec spec = new X509EncodedKeySpec(certificateData);
            KeyFactory kf = KeyFactory.getInstance("RSA");
            return (RSAPublicKey) kf.generatePublic(spec);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }


    public static byte[] encryptWithRSAPublicKey(byte[] source, RSAPublicKey key, String transformation) throws SQLException {
        try {
            Cipher cipher = Cipher.getInstance(transformation);
            cipher.init(Cipher.ENCRYPT_MODE, key);
            return cipher.doFinal(source);
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }


    public static long getClientFlagsExtendSha() {
        int flag = 0;
        flag |= Capabilities.CLIENT_LONG_PASSWORD;
        flag |= Capabilities.CLIENT_LONG_FLAG;
        flag |= Capabilities.CLIENT_CONNECT_WITH_DB;
        flag |= Capabilities.CLIENT_COMPRESS;
        flag |= Capabilities.CLIENT_ODBC;
        flag |= Capabilities.CLIENT_PROTOCOL_41;
        flag |= Capabilities.CLIENT_SSL;
        flag |= Capabilities.CLIENT_IGNORE_SIGPIPE;
        flag |= Capabilities.CLIENT_TRANSACTIONS;
        return flag;
    }

}
