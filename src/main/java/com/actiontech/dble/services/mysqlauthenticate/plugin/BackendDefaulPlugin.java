package com.actiontech.dble.services.mysqlauthenticate.plugin;

import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.mysql.HandshakeV10Packet;
import com.actiontech.dble.services.mysqlauthenticate.PluginName;

import static com.actiontech.dble.services.mysqlauthenticate.PluginName.unsupport_plugin;

/**
 * Created by szf on 2020/6/30.
 */
public class BackendDefaulPlugin extends MySQLAuthPlugin {

    public BackendDefaulPlugin(AbstractConnection connection) {
        super(connection);
    }

    @Override
    public void authenticate(String user, String password, String schema, byte packetId) {

    }

    @Override
    public PluginName handleData(byte[] data) {
        return null;
    }

    @Override
    public String handleBackData(byte[] data) {
        handshakePacket = new HandshakeV10Packet();
        handshakePacket.read(data);

        ((BackendConnection) connection).setThreadId(handshakePacket.getThreadId());
        connection.initCharacterSet(SystemConfig.getInstance().getCharset());

        String authPluginName = new String(handshakePacket.getAuthPluginName());
        return authPluginName;
    }


    @Override
    public void handleSwitchData(byte[] data) {

    }

    @Override
    public byte[] greeting() {
        return new byte[0];
    }

    @Override
    public PluginName getName() {
        return unsupport_plugin;
    }
}
