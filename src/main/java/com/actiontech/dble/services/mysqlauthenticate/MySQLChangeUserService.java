package com.actiontech.dble.services.mysqlauthenticate;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.connection.FrontendConnection;
import com.actiontech.dble.net.mysql.AuthSwitchRequestPackage;
import com.actiontech.dble.net.mysql.AuthSwitchResponsePackage;
import com.actiontech.dble.net.mysql.ChangeUserPacket;
import com.actiontech.dble.net.service.AuthResultInfo;
import com.actiontech.dble.services.BusinessService;
import com.actiontech.dble.services.FrontendService;
import com.actiontech.dble.services.mysqlauthenticate.util.AuthUtil;

/**
 * Created by szf on 2020/6/18.
 */
public class MySQLChangeUserService extends FrontendService {

    private final BusinessService frontendService;
    private volatile ChangeUserPacket changeUserPacket;

    public MySQLChangeUserService(AbstractConnection connection, BusinessService frontendService) {
        super(connection);
        this.frontendService = frontendService;
        this.seed = frontendService.getSeed();
    }

    @Override
    public void handleInnerData(byte[] data) {
        this.setPacketId(data[3]);
        if (changeUserPacket == null) {
            changeUserPacket = new ChangeUserPacket(frontendService.getClientCapabilities());
            changeUserPacket.read(data);
            if (PluginName.valueOf(changeUserPacket.getAuthPlugin()) == PluginName.mysql_native_password) {
                frontendService.resetConnection();
                sendSwitchPacket();
            } else {
                writeErrMessage(ErrorCode.ER_ACCESS_DENIED_ERROR, "unsupport auth plugin");
            }
        } else {
            handleSwitchResponse(data);
        }
    }

    private void sendSwitchPacket() {
        AuthSwitchRequestPackage authSwitch = new AuthSwitchRequestPackage(PluginName.mysql_native_password.toString().getBytes(), seed);
        authSwitch.setPacketId(this.nextPacketId());
        authSwitch.bufferWrite(connection);
    }

    private void handleSwitchResponse(byte[] data) {
        AuthSwitchResponsePackage authSwitchResponse = new AuthSwitchResponsePackage();
        authSwitchResponse.read(data);
        changeUserPacket.setPassword(authSwitchResponse.getAuthPluginData());
        checkForResult(AuthUtil.auth((FrontendConnection) connection, seed, PluginName.mysql_native_password, changeUserPacket));
    }

    private void checkForResult(AuthResultInfo info) {
        if (info.isSuccess()) {
            connection.setService(frontendService);
            frontendService.setUser(info.getUser());
            frontendService.setUserConfig(info.getUserConfig());
            frontendService.setSchema(changeUserPacket.getDatabase());
            frontendService.initCharsetIndex(changeUserPacket.getCharsetIndex());
            writeOkPacket();
        } else {
            this.writeErrMessage(ErrorCode.ER_ACCESS_DENIED_ERROR, info.getErrorMsg());
        }
    }

}
