package com.actiontech.dble.services.mysqlauthenticate;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.proto.handler.Impl.MySQLProtoHandlerImpl;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.net.service.AuthResultInfo;
import com.actiontech.dble.net.service.AuthService;
import com.actiontech.dble.services.MySQLBasedService;
import com.actiontech.dble.services.factorys.BusinessServiceFactory;
import com.actiontech.dble.services.mysqlauthenticate.plugin.CachingSHA2Pwd;
import com.actiontech.dble.services.mysqlauthenticate.plugin.MySQLAuthPlugin;
import com.actiontech.dble.services.mysqlauthenticate.plugin.NativePwd;
import com.actiontech.dble.singleton.TraceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.actiontech.dble.services.mysqlauthenticate.PluginName.caching_sha2_password;
import static com.actiontech.dble.services.mysqlauthenticate.PluginName.mysql_native_password;


/**
 * Created by szf on 2020/6/18.
 */
public class MySQLFrontAuthService extends MySQLBasedService implements AuthService {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLFrontAuthService.class);

    private volatile MySQLAuthPlugin plugin;

    private volatile byte[] seed;

    private volatile boolean hasAuthSwitched;

    public MySQLFrontAuthService(AbstractConnection connection) {
        super(connection);
        this.proto = new MySQLProtoHandlerImpl();
        plugin = MySQLAuthPlugin.getDefaultPlugin(connection);
    }


    @Override
    public void register() throws IOException {
        seed = plugin.greeting();
        connection.getSocketWR().asyncRead();
    }


    @Override
    public void handleInnerData(byte[] data) {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(this, "handle-auth-data");
        try {
            this.setPacketId(data[3]);
            if (data.length == QuitPacket.QUIT.length && data[4] == MySQLPacket.COM_QUIT) {
                connection.close("quit packet");
            } else if (data.length == PingPacket.PING.length && data[4] == PingPacket.COM_PING) {
                pingResponse();
            } else {
                if (hasAuthSwitched) {
                    //if got the switch response,check the result
                    plugin.handleSwitchData(data);
                    checkForResult(plugin.getInfo());
                } else {
                    switch (plugin.handleData(data)) {
                        case caching_sha2_password:
                            hasAuthSwitched = true;
                            this.plugin = new CachingSHA2Pwd(plugin);
                            requestToSwitch(caching_sha2_password);
                            break;
                        case mysql_native_password:
                            hasAuthSwitched = true;
                            this.plugin = new NativePwd(plugin);
                            requestToSwitch(mysql_native_password);
                            break;
                        case plugin_same_with_default:
                            checkForResult(plugin.getInfo());
                            break;
                        default:
                            //try to switch plugin to the default
                            requestToSwitch(plugin.getName());
                    }
                }
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
                packet.setPacketId(hasAuthSwitched ? 4 : 2);
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

    private void requestToSwitch(PluginName name) {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(this, "request-client-switch");
        try {
            AuthSwitchRequestPackage authSwitch = new AuthSwitchRequestPackage(name.toString().getBytes(), seed);
            authSwitch.setPacketId(this.nextPacketId());
            authSwitch.bufferWrite(connection);
        } finally {
            TraceManager.finishSpan(this, traceObject);
        }
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

}
