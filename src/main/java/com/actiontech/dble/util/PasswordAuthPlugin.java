package com.actiontech.dble.util;

import com.actiontech.dble.backend.mysql.SecurityUtil;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.MySQLInstance;
import com.actiontech.dble.net.AbstractConnection;
import com.actiontech.dble.net.mysql.BinaryPacket;
import com.actiontech.dble.net.mysql.HandshakeV10Packet;
import com.actiontech.dble.net.mysql.Reply323Packet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Base64;

public final class PasswordAuthPlugin {
    private static final Logger LOGGER = LoggerFactory.getLogger(PasswordAuthPlugin.class);


    private PasswordAuthPlugin() {
    }


    public static final byte[] GETPUBLICKEY = new byte[]{1, 0, 0, 3, 2};

    public static final byte[] PASS_WITH_PUBLICKEY = new byte[]{0, 1, 0, 5};

    public static final byte[] PASS_WITH_PUBLICKEY_NATIVE_FIRST = new byte[]{0, 1, 0, 7};

    public static final byte[] GETPUBLICKEY_NATIVE_FIRST = new byte[]{1, 0, 0, 5, 2};


    public static final byte[] WRITECACHINGPASSWORD = new byte[]{0x20, 0, 0, 3};

    public static final byte[] NATIVE_PASSWORD_WITH_PLUGINDATA = new byte[]{0x14, 0, 0, 3};

    public static final byte AUTH_SWITCH_PACKET = 0x01;

    public static final byte AUTHSTAGE_FAST_COMPLETE = 0x03;

    public static final byte AUTHSTAGE_FULL = 0x04;




    private static byte[] seedTotal = null;

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
            byte[] certificateData = Base64.getDecoder().decode(key.substring(offset, offset + len).replace("\n", ""));
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

    public static boolean sendEncryptedPassword(OutputStream out, InputStream in, byte[] authPluginData, byte[] getPublicKeyType, String password) throws Exception {
        boolean isConnected = true;
        if (Arrays.equals(getPublicKeyType, PasswordAuthPlugin.GETPUBLICKEY_NATIVE_FIRST)) {
            out.write(PasswordAuthPlugin.GETPUBLICKEY_NATIVE_FIRST);
        } else if (Arrays.equals(getPublicKeyType, PasswordAuthPlugin.GETPUBLICKEY)) {
            out.write(PasswordAuthPlugin.GETPUBLICKEY);
        } else {
            return false;
        }
        out.flush();
        BinaryPacket binKey = new BinaryPacket();
        binKey.readKey(in);
        byte[] publicKey = binKey.getPublicKey();
        byte[] input = password != null ? getBytesNullTerminated(password, "UTF-8") : new byte[]{0};
        byte[] mysqlScrambleBuff = new byte[input.length];
        if (Arrays.equals(getPublicKeyType, PasswordAuthPlugin.GETPUBLICKEY_NATIVE_FIRST)) {
            Security.xorString(input, mysqlScrambleBuff, authPluginData, input.length);
        } else if (Arrays.equals(getPublicKeyType, PasswordAuthPlugin.GETPUBLICKEY)) {
            Security.xorString(input, mysqlScrambleBuff, PasswordAuthPlugin.seedTotal, input.length);
        } else {
            return false;
        }
        byte[] encryptedPassword = PasswordAuthPlugin.encryptWithRSAPublicKey(mysqlScrambleBuff, PasswordAuthPlugin.decodeRSAPublicKey(new String(publicKey)), "RSA/ECB/OAEPWithSHA-1AndMGF1Padding");
        if (Arrays.equals(getPublicKeyType, PasswordAuthPlugin.GETPUBLICKEY_NATIVE_FIRST)) {
            out.write(PasswordAuthPlugin.PASS_WITH_PUBLICKEY_NATIVE_FIRST);
        } else if (Arrays.equals(getPublicKeyType, PasswordAuthPlugin.GETPUBLICKEY)) {
            out.write(PasswordAuthPlugin.PASS_WITH_PUBLICKEY);
        } else {
            return false;
        }
        out.write(encryptedPassword);
        out.flush();
        BinaryPacket resEncryBin = new BinaryPacket();
        resEncryBin.read(in);
        byte[] resEncResult = resEncryBin.getData();
        if (resEncResult[4] == 0x00) {
            isConnected = true;
        } else {
            MySQLInstance.logTestConnectionError(resEncResult);
            isConnected = false;
        }
        return isConnected;
    }


    public static void sendEnPaGetPub(byte[] getPublicKeyType, AbstractConnection s) throws Exception {
        if (Arrays.equals(getPublicKeyType, PasswordAuthPlugin.GETPUBLICKEY_NATIVE_FIRST)) {
            s.write(PasswordAuthPlugin.GETPUBLICKEY_NATIVE_FIRST);
        } else if (Arrays.equals(getPublicKeyType, PasswordAuthPlugin.GETPUBLICKEY)) {
            s.write(PasswordAuthPlugin.GETPUBLICKEY);
        } else {
            return;
        }
    }

    public static void sendEnPasswordWithPublicKey(byte[] authPluginData, byte[] getPublicKeyType, byte[] publicKey, MySQLConnection s) throws Exception {
        byte[] input = s.getPassword() != null ? getBytesNullTerminated(s.getPassword(), "UTF-8") : new byte[]{0};
        byte[] mysqlScrambleBuff = new byte[input.length];
        if (Arrays.equals(getPublicKeyType, PasswordAuthPlugin.GETPUBLICKEY_NATIVE_FIRST)) {
            Security.xorString(input, mysqlScrambleBuff, authPluginData, input.length);
        } else if (Arrays.equals(getPublicKeyType, PasswordAuthPlugin.GETPUBLICKEY)) {
            Security.xorString(input, mysqlScrambleBuff, PasswordAuthPlugin.seedTotal, input.length);
        } else {
            return;
        }
        byte[] encryptedPassword = PasswordAuthPlugin.encryptWithRSAPublicKey(mysqlScrambleBuff, PasswordAuthPlugin.decodeRSAPublicKey(new String(publicKey)), "RSA/ECB/OAEPWithSHA-1AndMGF1Padding");
        if (Arrays.equals(getPublicKeyType, PasswordAuthPlugin.GETPUBLICKEY_NATIVE_FIRST)) {
            s.write(combineHeaderAndPassword(PasswordAuthPlugin.PASS_WITH_PUBLICKEY_NATIVE_FIRST, encryptedPassword));
        } else if (Arrays.equals(getPublicKeyType, PasswordAuthPlugin.GETPUBLICKEY)) {
            s.write(combineHeaderAndPassword(PasswordAuthPlugin.PASS_WITH_PUBLICKEY, encryptedPassword));
        } else {
            return;
        }
    }


    public static byte[] getBytesNullTerminated(String value, String encoding) {
        // Charset cs = findCharset(encoding);
        Charset cs = StandardCharsets.UTF_8;
        ByteBuffer buf = cs.encode(value);
        int encodedLen = buf.limit();
        byte[] asBytes = new byte[encodedLen + 1];
        buf.get(asBytes, 0, encodedLen);
        asBytes[encodedLen] = 0;
        return asBytes;
    }

    public static void send323AuthPacket(OutputStream out, BinaryPacket bin2, HandshakeV10Packet handshake, String password) throws Exception {
        LOGGER.warn("Client don't support the MySQL 323 plugin ");
        Reply323Packet r323 = new Reply323Packet();
        r323.setPacketId((byte) (bin2.getPacketId() + 1));
        if (password != null && password.length() > 0) {
            r323.setSeed(SecurityUtil.scramble323(password, new String(handshake.getSeed())).getBytes());
        }
        r323.write(out);
        out.flush();
    }

    public static byte[] cachingSha2Password(byte[] cs2p) {
        return combineHeaderAndPassword(WRITECACHINGPASSWORD, cs2p);
    }


    public static byte[] nativePassword(byte[] cs2p) {
        return combineHeaderAndPassword(NATIVE_PASSWORD_WITH_PLUGINDATA, cs2p);
    }


    public static byte[] combineHeaderAndPassword(byte[] des1, byte[] des2) {
        byte[] target = new byte[des1.length + des2.length];
        int i ;
        for (i = 0 ; i < des1.length ; i++) {
            target[i] = des1[i];
        }
        for (int j = 0 ; j < des2.length ; j++, i++) {
            target[i] = des2[j];
        }
        return target;
    }

}
