/*
* Copyright (C) 2016-2018 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.net.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.SecurityUtil;
import com.actiontech.dble.config.Capabilities;
import com.actiontech.dble.config.ErrorCode;
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

        AuthPacket auth = new AuthPacket();
        auth.read(data);

        //check mysql_native_password
        if (auth.getAuthPlugin() != null && !"mysql_native_password".equals(auth.getAuthPlugin())) {
            failure(ErrorCode.ER_ACCESS_DENIED_ERROR, "only mysql_native_password auth check is supported");
            return;
        }

        // check user
        if (!checkUser(auth.getUser(), source.getHost())) {
            failure(ErrorCode.ER_ACCESS_DENIED_ERROR, "Access denied for user '" + auth.getUser() + "' with host '" + source.getHost() + "'");
            return;
        }

        // check password
        if (!checkPassword(auth.getPassword(), auth.getUser())) {
            failure(ErrorCode.ER_ACCESS_DENIED_ERROR, "Access denied for user '" + auth.getUser() + "', because password is error ");
            return;
        }


        // check dataHost without writeHost flag
        if (DbleServer.getInstance().getConfig().isDataHostWithoutWR() && !(this instanceof ManagerAuthenticator)) {
            failure(ErrorCode.ER_ACCESS_DENIED_ERROR, "Access denied for user '" + auth.getUser() + "', because there are some dataHost is empty ");
            return;
        }

        // check schema
        switch (checkSchema(auth.getDatabase(), auth.getUser())) {
            case ErrorCode.ER_BAD_DB_ERROR:
                failure(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + auth.getDatabase() + "'");
                break;
            case ErrorCode.ER_DBACCESS_DENIED_ERROR:
                String s = "Access denied for user '" + auth.getUser() + "' to database '" + auth.getDatabase() + "'";
                failure(ErrorCode.ER_DBACCESS_DENIED_ERROR, s);
                break;
            default:
                break;
        }

        //check maxconnection
        switch (DbleServer.getInstance().getUserManager().maxConnectionCheck(auth.getUser(), source.getPrivileges().getMaxCon(auth.getUser()))) {
            case SERVER_MAX:
                String s = "Access denied for user '" + auth.getUser() + "',too many connections for dble server";
                failure(ErrorCode.ER_ACCESS_DENIED_ERROR, s);
                break;
            case USER_MAX:
                String s1 = "Access denied for user '" + auth.getUser() + "',too many connections for this user";
                failure(ErrorCode.ER_ACCESS_DENIED_ERROR, s1);
                break;
            default:
                success(auth);
        }


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
        source.setCharsetIndex(auth.getCharsetIndex());
        source.setHandler(successCommendHandler());

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
        source.write(source.writeToBuffer(AUTH_OK, buffer));
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
