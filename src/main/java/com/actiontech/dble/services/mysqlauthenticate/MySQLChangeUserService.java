package com.actiontech.dble.services.mysqlauthenticate;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.user.UserName;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.connection.FrontendConnection;
import com.actiontech.dble.net.mysql.AuthSwitchRequestPackage;
import com.actiontech.dble.net.mysql.AuthSwitchResponsePackage;
import com.actiontech.dble.net.mysql.ChangeUserPacket;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.services.BusinessService;
import com.actiontech.dble.services.FrontEndService;
import com.actiontech.dble.services.mysqlauthenticate.util.AuthUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by szf on 2020/6/18.
 */
public class MySQLChangeUserService extends FrontEndService {
    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLChangeUserService.class);

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
                frontendService.innerCleanUp();
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
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("changeUser AuthSwitch response");
        }
        AuthSwitchResponsePackage authSwitchResponse = new AuthSwitchResponsePackage();
        authSwitchResponse.read(data);
        changeUserPacket.setPassword(authSwitchResponse.getAuthPluginData());
        String errMsg = AuthUtil.auth((FrontendConnection) connection, seed, PluginName.mysql_native_password, changeUserPacket);
        byte packetId = (byte) (authSwitchResponse.getPacketId() + 1);
        if (errMsg == null) {
            changeUserSuccess(changeUserPacket, packetId);
        } else {
            writeErrMessage(packetId, ErrorCode.ER_ACCESS_DENIED_ERROR, errMsg);
        }
    }

    private void changeUserSuccess(ChangeUserPacket newUser, byte packetId) {
        connection.setService(frontendService);
        UserName user = new UserName(newUser.getUser(), newUser.getTenant());
        frontendService.setUser(user);
        frontendService.setUserConfig(DbleServer.getInstance().getConfig().getUsers().get(user));
        frontendService.setSchema(newUser.getDatabase());
        frontendService.getConnection().initCharsetIndex(newUser.getCharsetIndex());
        OkPacket ok = new OkPacket();
        ok.read(OkPacket.OK);
        ok.setPacketId(packetId);
        ok.write(connection);
    }

    @Override
    public String getExecuteSql() {
        return null;
    }
}
