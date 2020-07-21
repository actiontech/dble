package com.actiontech.dble.services.mysqlauthenticate.plugin;

import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.config.Capabilities;
import com.actiontech.dble.config.Versions;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.mysql.AuthPacket;
import com.actiontech.dble.net.mysql.AuthSwitchResponsePackage;
import com.actiontech.dble.net.mysql.HandshakeV10Packet;
import com.actiontech.dble.net.service.AuthResultInfo;
import com.actiontech.dble.services.mysqlauthenticate.PluginName;
import com.actiontech.dble.util.RandomUtil;

import static com.actiontech.dble.services.mysqlauthenticate.PluginName.caching_sha2_password;
import static com.actiontech.dble.services.mysqlauthenticate.PluginName.mysql_native_password;


/**
 * Created by szf on 2020/6/18.
 */
public abstract class MySQLAuthPlugin {

    public static final PluginName[] MYSQL_DEFAULT_PLUGIN = {mysql_native_password, mysql_native_password, mysql_native_password, caching_sha2_password};
    protected byte[] seed;
    protected final AbstractConnection connection;
    protected AuthResultInfo info;
    protected AuthPacket authPacket;
    protected HandshakeV10Packet handshakePacket;
    protected volatile byte[] authPluginData;

    MySQLAuthPlugin(AbstractConnection connection) {
        this.connection = connection;
    }

    public MySQLAuthPlugin(MySQLAuthPlugin plugin) {
        this.authPluginData = plugin.authPluginData;
        this.seed = plugin.seed;
        this.authPacket = plugin.authPacket;
        this.connection = plugin.connection;
        this.handshakePacket = plugin.handshakePacket;
    }


    public abstract void authenticate(String user, String password, String schema, byte packetId);

    public abstract PluginName handleData(byte[] data);

    public abstract PluginName handleBackData(byte[] data) throws Exception;

    public abstract void handleSwitchData(byte[] data);

    public byte[] greeting() {
        // generate auth data
        byte[] rand1 = RandomUtil.randomBytes(8);
        byte[] rand2 = RandomUtil.randomBytes(12);

        // save  auth data
        byte[] rand = new byte[rand1.length + rand2.length];
        System.arraycopy(rand1, 0, rand, 0, rand1.length);
        System.arraycopy(rand2, 0, rand, rand1.length, rand2.length);
        this.seed = rand;

        HandshakeV10Packet hs = new HandshakeV10Packet();
        hs.setPacketId(0);
        hs.setProtocolVersion(Versions.PROTOCOL_VERSION);  // [0a] protocol version   V10
        hs.setServerVersion(Versions.getServerVersion());
        hs.setThreadId(connection.getId());
        hs.setSeed(rand1);
        hs.setServerCapabilities(getServerCapabilities());
        int charsetIndex = CharsetUtil.getCharsetDefaultIndex(SystemConfig.getInstance().getCharset());
        hs.setServerCharsetIndex((byte) (charsetIndex & 0xff));
        hs.setServerStatus(2);
        hs.setRestOfScrambleBuff(rand2);
        hs.setAuthPluginName(this.getName().toString().getBytes());

        //writeDirectly out
        hs.write(connection);
        return seed;
    }

    public abstract PluginName getName();

    public AuthResultInfo getInfo() {
        return info;
    }

    public byte[] getSeed() {
        return seed;
    }


    public HandshakeV10Packet getHandshakePacket() {
        return handshakePacket;
    }

    public void setHandshakePacket(HandshakeV10Packet handshakePacket) {
        this.handshakePacket = handshakePacket;
    }

    protected int getServerCapabilities() {
        int flag = 0;
        flag |= Capabilities.CLIENT_LONG_PASSWORD;
        flag |= Capabilities.CLIENT_FOUND_ROWS;
        flag |= Capabilities.CLIENT_LONG_FLAG;
        flag |= Capabilities.CLIENT_CONNECT_WITH_DB;
        // flag |= Capabilities.CLIENT_NO_SCHEMA;
        boolean usingCompress = SystemConfig.getInstance().getUseCompression() == 1;
        if (usingCompress) {
            flag |= Capabilities.CLIENT_COMPRESS;
        }

        flag |= Capabilities.CLIENT_ODBC;
        flag |= Capabilities.CLIENT_LOCAL_FILES;
        flag |= Capabilities.CLIENT_IGNORE_SPACE;
        flag |= Capabilities.CLIENT_PROTOCOL_41;
        flag |= Capabilities.CLIENT_INTERACTIVE;
        // flag |= Capabilities.CLIENT_SSL;
        flag |= Capabilities.CLIENT_IGNORE_SIGPIPE;
        flag |= Capabilities.CLIENT_TRANSACTIONS;
        // flag |= ServerDefs.CLIENT_RESERVED;
        flag |= Capabilities.CLIENT_SECURE_CONNECTION;
        flag |= Capabilities.CLIENT_MULTI_STATEMENTS;
        flag |= Capabilities.CLIENT_MULTI_RESULTS;
        flag |= Capabilities.CLIENT_PLUGIN_AUTH;
        flag |= Capabilities.CLIENT_CONNECT_ATTRS;
        return flag;
    }


    protected void sendAuthPacket(AuthPacket packet, byte[] authPassword, String authPluginName, String schema) {
        packet.setPassword(authPassword);
        packet.setClientFlags(getClientFlagSha());
        packet.setAuthPlugin(authPluginName);
        packet.setDatabase(schema);
        packet.bufferWrite(this.connection);
    }

    protected void sendAuthPacket(AuthSwitchResponsePackage packet, byte[] authData, byte packetId) {
        packet.setAuthPluginData(authData);
        packet.setPacketId(packetId);
        packet.setPacketLength(authData.length);
        packet.bufferWrite(this.connection);
    }

    private long getClientFlagSha() {
        int flag = 0;
        flag |= initClientFlags();
        flag |= Capabilities.CLIENT_PLUGIN_AUTH;
        return flag;
    }

    public static MySQLAuthPlugin getDefaultPlugin(AbstractConnection connection) {
        String majorMySQLVersion = SystemConfig.getInstance().getFakeMySQLVersion();
        if (majorMySQLVersion != null) {
            String[] versions = majorMySQLVersion.split("\\.");
            if (versions.length == 3) {
                majorMySQLVersion = versions[0] + "." + versions[1];
                for (int i = 0; i < SystemConfig.MYSQL_VERSIONS.length; i++) {
                    // version is x.y.z ,just compare the x.y
                    if (majorMySQLVersion.equals(SystemConfig.MYSQL_VERSIONS[i])) {
                        switch (MYSQL_DEFAULT_PLUGIN[i]) {
                            case mysql_native_password:
                                return new NativePwd(connection);
                            case caching_sha2_password:
                                return new CachingSHA2Pwd(connection);
                            default:
                                return new NativePwd(connection);
                        }
                    }
                }
            }
        } else {
            return new NativePwd(connection);
        }
        return null;
    }

    private static long initClientFlags() {
        int flag = 0;
        flag |= Capabilities.CLIENT_LONG_PASSWORD;
        flag |= Capabilities.CLIENT_FOUND_ROWS;
        flag |= Capabilities.CLIENT_LONG_FLAG;
        flag |= Capabilities.CLIENT_CONNECT_WITH_DB;
        // flag |= Capabilities.CLIENT_NO_SCHEMA;
        boolean usingCompress = SystemConfig.getInstance().getUseCompression() == 1;
        if (usingCompress) {
            flag |= Capabilities.CLIENT_COMPRESS;
        }
        flag |= Capabilities.CLIENT_ODBC;
        flag |= Capabilities.CLIENT_LOCAL_FILES;
        flag |= Capabilities.CLIENT_IGNORE_SPACE;
        flag |= Capabilities.CLIENT_PROTOCOL_41;
        flag |= Capabilities.CLIENT_INTERACTIVE;
        // flag |= Capabilities.CLIENT_SSL;
        flag |= Capabilities.CLIENT_IGNORE_SIGPIPE;
        flag |= Capabilities.CLIENT_TRANSACTIONS;
        // flag |= Capabilities.CLIENT_RESERVED;
        flag |= Capabilities.CLIENT_SECURE_CONNECTION;
        // client extension
        flag |= Capabilities.CLIENT_MULTI_STATEMENTS;
        flag |= Capabilities.CLIENT_MULTI_RESULTS;
        return flag;
    }

}
