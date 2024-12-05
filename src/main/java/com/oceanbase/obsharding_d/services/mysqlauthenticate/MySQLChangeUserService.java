/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.mysqlauthenticate;

import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.net.connection.AbstractConnection;
import com.oceanbase.obsharding_d.net.connection.FrontendConnection;
import com.oceanbase.obsharding_d.net.mysql.AuthSwitchRequestPackage;
import com.oceanbase.obsharding_d.net.mysql.AuthSwitchResponsePackage;
import com.oceanbase.obsharding_d.net.mysql.ChangeUserPacket;
import com.oceanbase.obsharding_d.net.service.AuthResultInfo;
import com.oceanbase.obsharding_d.services.BusinessService;
import com.oceanbase.obsharding_d.services.FrontendService;
import com.oceanbase.obsharding_d.services.mysqlauthenticate.util.AuthUtil;

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
    protected void handleInnerData(byte[] data) {
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
