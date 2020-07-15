/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.net.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.user.RwSplitUserConfig;
import com.actiontech.dble.config.model.user.UserConfig;
import com.actiontech.dble.config.model.user.UserName;
import com.actiontech.dble.config.util.AuthUtil;
import com.actiontech.dble.net.FrontendConnection;
import com.actiontech.dble.net.NIOHandler;
import com.actiontech.dble.net.mysql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * FrontendAuthenticator
 *
 * @author mycat
 */
public abstract class FrontendAuthenticator implements NIOHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(FrontendAuthenticator.class);
    private static final byte[] AUTH_OK = new byte[]{7, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0};
    private static final byte[] SWITCH_AUTH_OK = new byte[]{7, 0, 0, 4, 0, 0, 0, 2, 0, 0, 0};
    private AuthPacket authPacket;
    private boolean isAuthSwitch = false;

    protected final FrontendConnection source;

    public FrontendAuthenticator(FrontendConnection source) {
        this.source = source;
    }

    @Override
    public void handle(byte[] data) {
        DbleServer.getInstance().getComplexQueryExecutor().execute(new Runnable() {
            @Override
            public void run() {
                asynchronousHandle(data);
            }
        });
    }

    private void asynchronousHandle(byte[] data) {
        // check quit packet
        if (data.length == QuitPacket.QUIT.length && data[4] == MySQLPacket.COM_QUIT) {
            source.close("quit packet");
            return;
        } else if (data.length == PingPacket.PING.length && data[4] == PingPacket.COM_PING) {
            if (DbleServer.getInstance().isOnline()) {
                source.write(AUTH_OK);
            } else {
                ErrorPacket errPacket = new ErrorPacket();
                errPacket.setErrNo(ErrorCode.ER_YES);
                errPacket.setMessage("server is offline.".getBytes());
                //close the mysql connection if error occur
                errPacket.setPacketId(2);
                source.write(errPacket.toBytes());
            }
            return;
        }

        if (isAuthSwitch) {
            // receive switch auth response package
            AuthSwitchResponsePackage authSwitchResponse = new AuthSwitchResponsePackage();
            authSwitchResponse.read(data);
            authPacket.setPassword(authSwitchResponse.getAuthPluginData());
        } else {
            AuthPacket auth = new AuthPacket();
            auth.read(data);
            authPacket = auth;
            if (auth.getAuthPlugin() != null && !"mysql_native_password".equals(auth.getAuthPlugin())) {
                // send switch auth request package
                AuthSwitchRequestPackage authSwitch = new AuthSwitchRequestPackage("mysql_native_password".getBytes(), this.source.getSeed());
                authSwitch.setPacketId(auth.getPacketId() + 1);
                isAuthSwitch = true;
                authSwitch.write(source);
                return;
            }
        }

        // check mysql client user
        String errMsg = AuthUtil.authority(source, new UserName(authPacket.getUser(), authPacket.getTenant()), authPacket.getPassword(), authPacket.getDatabase(), this instanceof ManagerAuthenticator);
        //this version is not support rwSplitUser
        if (errMsg == null) {
            errMsg = rejectRwSplitUser(authPacket);
        }
        if (errMsg == null) {
            success(authPacket);
        } else {
            failure(ErrorCode.ER_ACCESS_DENIED_ERROR, errMsg);
        }
    }

    private void success(AuthPacket auth) {
        setConnProperties(auth);
        ByteBuffer buffer = source.allocate();
        if (isAuthSwitch) {
            source.write(source.writeToBuffer(SWITCH_AUTH_OK, buffer));
        } else {
            source.write(source.writeToBuffer(AUTH_OK, buffer));
        }
    }

    protected abstract void setConnProperties(AuthPacket auth);


    protected void failure(int errNo, String info) {
        LOGGER.info(source.toString() + info);
        if (isAuthSwitch) {
            source.writeErrMessage((byte) 4, errNo, info);
        } else {
            source.writeErrMessage((byte) 2, errNo, info);
        }
    }

    //todo: delete next version
    private String rejectRwSplitUser(AuthPacket auth) {
        UserName user = new UserName(auth.getUser(), auth.getTenant());
        UserConfig userConfig = DbleServer.getInstance().getConfig().getUsers().get(user);
        if (userConfig instanceof RwSplitUserConfig) {
            return "this version does not support rwSplitUser";
        }
        return null;
    }
}
