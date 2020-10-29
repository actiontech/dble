package com.actiontech.dble.services.mysqlauthenticate;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.config.Capabilities;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Versions;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.user.UserConfig;
import com.actiontech.dble.config.model.user.UserName;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.net.service.AuthResultInfo;
import com.actiontech.dble.net.service.AuthService;
import com.actiontech.dble.services.factorys.BusinessServiceFactory;
import com.actiontech.dble.services.mysqlauthenticate.util.AuthUtil;
import com.actiontech.dble.singleton.TraceManager;
import com.actiontech.dble.util.RandomUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


/**
 * Created by szf on 2020/6/18.
 */
public class MySQLFrontAuthService extends AuthService {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLFrontAuthService.class);

    private volatile AuthPacket authPacket;

    public MySQLFrontAuthService(AbstractConnection connection) {
        super(connection);
        this.pluginName = getDefaultPluginName();
    }

    @Override
    public void register() throws IOException {
        greeting();
        this.connection.getSocketWR().asyncRead();
    }

    @Override
    public void handleInnerData(byte[] data) {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(this, "handle-auth-data");
        try {
            this.setPacketId(data[3]);
            if (data.length == QuitPacket.QUIT.length && data[4] == MySQLPacket.COM_QUIT) {
                connection.close("quit packet");
                return;
            } else if (data.length == PingPacket.PING.length && data[4] == PingPacket.COM_PING) {
                pingResponse();
                return;
            }

            if (needAuthSwitched) {
                handleSwitchResponse(data);
            } else {
                handleAuthPacket(data);
            }

        } finally {
            TraceManager.finishSpan(this, traceObject);
        }
    }

    private void checkForResult(AuthResultInfo info) {
        if (info == null) {
            return;
        }
        TraceManager.serviceTrace(this, "check-auth-result");
        try {
            if (info.isSuccess()) {
                AbstractService service = BusinessServiceFactory.getBusinessService(info, connection);
                connection.setService(service);
                MySQLPacket packet = new OkPacket();
                packet.setPacketId(needAuthSwitched ? 4 : 2);
                packet.write(connection);
            } else {
                writeOutErrorMessage(info.getErrorMsg());
            }
        } finally {
            TraceManager.sessionFinish(this);
        }
    }

    private void writeOutErrorMessage(String errorMsg) {
        this.writeErrMessage(ErrorCode.ER_ACCESS_DENIED_ERROR, errorMsg);
    }

    private void pingResponse() {
        if (DbleServer.getInstance().isOnline()) {
            OkPacket okPacket = new OkPacket();
            okPacket.setPacketId(2);
            this.write(okPacket);
        } else {
            ErrorPacket errPacket = new ErrorPacket();
            errPacket.setErrNo(ErrorCode.ER_YES);
            errPacket.setMessage("server is offline.".getBytes());
            //close the mysql connection if error occur
            errPacket.setPacketId(2);
            this.write(errPacket);
        }
    }

    @Override
    public void onConnectFailed(Throwable e) {

    }


    private void greeting() {
        // generate auth data
        byte[] rand1 = RandomUtil.randomBytes(8);
        byte[] rand2 = RandomUtil.randomBytes(12);

        // save auth data
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
        hs.setAuthPluginName(pluginName.name().getBytes());

        //writeDirectly out
        hs.write(connection);
    }

    private void handleAuthPacket(byte[] data) {
        AuthPacket auth = new AuthPacket();
        auth.read(data);
        this.authPacket = auth;
        try {
            PluginName name = PluginName.valueOf(auth.getAuthPlugin());
            if (pluginName != name) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("auth switch request client-plugin:[{}],server-plugin:[{}]->[{}]", name, pluginName, PluginName.mysql_native_password);
                }
                needAuthSwitched = true;
                this.pluginName = PluginName.mysql_native_password;
                sendSwitchPacket(pluginName);
                return;
            }
            // check user and password whether is correct
            auth();
        } catch (IllegalArgumentException e) {
            needAuthSwitched = true;
            sendSwitchPacket(pluginName);
        }
    }

    private void sendSwitchPacket(PluginName name) {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(this, "request-client-switch");
        try {
            AuthSwitchRequestPackage authSwitch = new AuthSwitchRequestPackage(name.toString().getBytes(), seed);
            authSwitch.setPacketId(this.nextPacketId());
            authSwitch.bufferWrite(connection);
        } finally {
            TraceManager.finishSpan(this, traceObject);
        }
    }

    private void handleSwitchResponse(byte[] data) {
        AuthSwitchResponsePackage authSwitchResponse = new AuthSwitchResponsePackage();
        authSwitchResponse.read(data);
        authPacket.setPassword(authSwitchResponse.getAuthPluginData());

        // check user and password whether is correct
        auth();
    }

    private void auth() {
        String errMsg = AuthUtil.auth(new UserName(authPacket.getUser(), authPacket.getTenant()), connection, seed, authPacket.getPassword(), authPacket.getDatabase(), pluginName, authPacket.getClientFlags());
        UserConfig userConfig = DbleServer.getInstance().getConfig().getUsers().get(new UserName(authPacket.getUser(), authPacket.getTenant()));
        checkForResult(new AuthResultInfo(errMsg, authPacket, userConfig));
    }

    private int getServerCapabilities() {
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

}
