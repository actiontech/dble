package com.actiontech.dble.services.mysqlauthenticate.plugin;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.user.UserConfig;
import com.actiontech.dble.config.model.user.UserName;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.net.service.AuthResultInfo;
import com.actiontech.dble.services.mysqlauthenticate.PasswordAuthPlugin;
import com.actiontech.dble.services.mysqlauthenticate.PluginName;
import com.actiontech.dble.services.mysqlauthenticate.util.AuthUtil;

import java.security.NoSuchAlgorithmException;

import static com.actiontech.dble.services.mysqlauthenticate.PluginName.mysql_native_password;


/**
 * Created by szf on 2020/6/18.
 */
public class NativePwd extends MySQLAuthPlugin {

    private final PluginName pluginName = mysql_native_password;

    public NativePwd(AbstractConnection connection) {
        super(connection);
    }

    public NativePwd(MySQLAuthPlugin plugin) {
        super(plugin);
    }

    @Override
    public void authenticate(String user, String password, String schema, byte packetId) {
        AuthPacket packet = new AuthPacket();
        packet.setPacketId(packetId);
        packet.setMaxPacketSize(connection.getMaxPacketSize());
        int charsetIndex = CharsetUtil.getCharsetDefaultIndex(SystemConfig.getInstance().getCharset());
        packet.setCharsetIndex(charsetIndex);
        packet.setUser(user);
        try {
            if (authPluginData == null) {
                sendAuthPacket(packet, PasswordAuthPlugin.passwd(password, handshakePacket), pluginName.name(), schema);
            } else {
                sendAuthPacket(new AuthSwitchResponsePackage(), PasswordAuthPlugin.passwd(password, handshakePacket), packetId);
            }
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public PluginName handleBackData(byte[] data) throws Exception {
        switch (data[4]) {
            case AuthSwitchRequestPackage.STATUS:
                BinaryPacket bin2 = new BinaryPacket();
                String authPluginName = bin2.getAuthPluginName(data);
                authPluginData = bin2.getAuthPluginData(data);
                try {
                    PluginName name = PluginName.valueOf(authPluginName);
                    return name;
                } catch (IllegalArgumentException e) {
                    return PluginName.unsupport_plugin;
                }
            case OkPacket.FIELD_COUNT:
                // execute auth response
                info = new AuthResultInfo(null);
                return PluginName.plugin_same_with_default;
            case ErrorPacket.FIELD_COUNT:
                ErrorPacket err = new ErrorPacket();
                err.read(data);
                String errMsg = new String(err.getMessage());
                info = new AuthResultInfo(errMsg);
                return PluginName.plugin_same_with_default;
            default:
                return PluginName.unsupport_plugin;
        }
    }


    @Override
    public PluginName handleData(byte[] data) {
        AuthPacket auth = new AuthPacket();
        auth.read(data);
        authPacket = auth;
        try {
            PluginName name = PluginName.valueOf(auth.getAuthPlugin());
            if (pluginName == name) {
                String errMsg = AuthUtil.auhth(new UserName(authPacket.getUser(), authPacket.getTenant()), connection, seed, authPacket.getPassword(), authPacket.getDatabase(), pluginName);
                UserConfig userConfig = DbleServer.getInstance().getConfig().getUsers().get(new UserName(authPacket.getUser(), authPacket.getTenant()));
                info = new AuthResultInfo(errMsg, authPacket, userConfig);
                return PluginName.plugin_same_with_default;
            } else {
                return name;
            }
        } catch (IllegalArgumentException e) {
            return PluginName.unsupport_plugin;
        }
    }

    @Override
    public void handleSwitchData(byte[] data) {
        AuthSwitchResponsePackage authSwitchResponse = new AuthSwitchResponsePackage();
        authSwitchResponse.read(data);
        authPacket.setPassword(authSwitchResponse.getAuthPluginData());

        String errMsg = AuthUtil.auhth(new UserName(authPacket.getUser(), authPacket.getTenant()), connection, seed, authPacket.getPassword(), authPacket.getDatabase(), pluginName);

        UserConfig userConfig = DbleServer.getInstance().getConfig().getUsers().get(new UserName(authPacket.getUser(), authPacket.getTenant()));
        info = new AuthResultInfo(errMsg, authPacket, userConfig);
    }


    @Override
    public PluginName getName() {
        return mysql_native_password;
    }

}
