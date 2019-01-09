/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.net.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.SecurityUtil;
import com.actiontech.dble.config.Capabilities;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.FrontendConnection;
import com.actiontech.dble.net.NIOHandler;
import com.actiontech.dble.net.mysql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.Set;

/**
 * FrontendAuthenticator
 *
 * @author mycat
 */
public class FrontendAuthenticator implements NIOHandler {

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
        if (authority(authPacket.getUser(), authPacket.getPassword(), authPacket.getDatabase())) {
            success(authPacket);
        }
    }

    private boolean authority(String user, byte[] pwd, String schema) {

        // check user
        if (!checkUser(user, source.getHost())) {
            failure(ErrorCode.ER_ACCESS_DENIED_ERROR, "Access denied for user '" + user + "' with host '" + source.getHost() + "'");
            return false;
        }

        // check password
        if (!checkPassword(pwd, user)) {
            failure(ErrorCode.ER_ACCESS_DENIED_ERROR, "Access denied for user '" + user + "', because password is error ");
            return false;
        }

        // check dataHost without writeHost flag
        if (DbleServer.getInstance().getConfig().isDataHostWithoutWR() && !(this instanceof ManagerAuthenticator)) {
            failure(ErrorCode.ER_ACCESS_DENIED_ERROR, "Access denied for user '" + user + "', because there are some dataHost is empty ");
            return false;
        }

        // check schema
        switch (checkSchema(schema, user)) {
            case ErrorCode.ER_BAD_DB_ERROR:
                failure(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + schema + "'");
                return false;
            case ErrorCode.ER_DBACCESS_DENIED_ERROR:
                String s = "Access denied for user '" + user + "' to database '" + schema + "'";
                failure(ErrorCode.ER_DBACCESS_DENIED_ERROR, s);
                return false;
            default:
                break;
        }

        //check maxconnection
        switch (DbleServer.getInstance().getUserManager().maxConnectionCheck(user, source.getPrivileges().getMaxCon(user), (source instanceof ManagerConnection))) {
            case SERVER_MAX:
                String s = "Access denied for user '" + user + "',too many connections for dble server";
                failure(ErrorCode.ER_ACCESS_DENIED_ERROR, s);
                return false;
            case USER_MAX:
                String s1 = "Access denied for user '" + user + "',too many connections for this user";
                failure(ErrorCode.ER_ACCESS_DENIED_ERROR, s1);
                return false;
            default:
                break;
        }
        return true;
    }

    protected boolean checkUser(String user, String host) {
        return source.getPrivileges().userExists(user, host);
    }

    protected boolean checkPassword(byte[] password, String user) {
        String pass = source.getPrivileges().getPassword(user);

        // check null
        if (pass == null || pass.length() == 0) {
            return password == null || password.length == 0;
        }
        if (password == null || password.length == 0) {
            return false;
        }

        // encrypt
        byte[] encryptPass = null;
        try {
            encryptPass = SecurityUtil.scramble411(pass.getBytes(), source.getSeed());
        } catch (NoSuchAlgorithmException e) {
            LOGGER.info(source.toString(), e);
            return false;
        }
        if (encryptPass != null && (encryptPass.length == password.length)) {
            int i = encryptPass.length;
            while (i-- != 0) {
                if (encryptPass[i] != password[i]) {
                    return false;
                }
            }
        } else {
            return false;
        }

        return true;
    }

    protected int checkSchema(String schema, String user) {
        if (schema == null) {
            return 0;
        }
        if (DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
            schema = schema.toLowerCase();
        }
        FrontendPrivileges privileges = source.getPrivileges();
        if (!privileges.schemaExists(schema)) {
            return ErrorCode.ER_BAD_DB_ERROR;
        }
        Set<String> schemas = privileges.getUserSchemas(user);
        if (schemas == null || schemas.size() == 0 || schemas.contains(schema)) {
            return 0;
        } else {
            return ErrorCode.ER_DBACCESS_DENIED_ERROR;
        }
    }

    protected NIOHandler successCommendHandler() {
        return new FrontendCommandHandler(source);
    }

    protected void success(AuthPacket auth) {
        source.setAuthenticated(true);
        source.setUser(auth.getUser());
        source.setSchema(auth.getDatabase());
        source.initCharsetIndex(auth.getCharsetIndex());
        source.setHandler(successCommendHandler());
        source.setMultStatementAllow(auth.isMultStatementAllow());

        if (LOGGER.isDebugEnabled()) {
            StringBuilder s = new StringBuilder();
            s.append(source).append('\'').append(auth.getUser()).append("' login success");
            byte[] extra = auth.getExtra();
            if (extra != null && extra.length > 0) {
                s.append(",extra:").append(new String(extra));
            }
            LOGGER.debug(s.toString());
        }

        ByteBuffer buffer = source.allocate();
        if (isAuthSwitch) {
            source.write(source.writeToBuffer(SWITCH_AUTH_OK, buffer));
        } else {
            source.write(source.writeToBuffer(AUTH_OK, buffer));
        }
        boolean clientCompress = Capabilities.CLIENT_COMPRESS == (Capabilities.CLIENT_COMPRESS & auth.getClientFlags());
        boolean usingCompress = DbleServer.getInstance().getConfig().getSystem().getUseCompression() == 1;
        if (clientCompress && usingCompress) {
            source.setSupportCompress(true);
        }
    }

    protected void failure(int errNo, String info) {
        LOGGER.info(source.toString() + info);
        source.writeErrMessage((byte) 2, errNo, info);
    }
}
