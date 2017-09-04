/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese
 * opensource volunteers. you can redistribute it and/or modify it under the
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Any questions about this component can be directed to it's project Web address
 * https://code.google.com/p/opencloudb/.
 *
 */
package com.actiontech.dble.net.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.SecurityUtil;
import com.actiontech.dble.config.Capabilities;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.FrontendConnection;
import com.actiontech.dble.net.NIOHandler;
import com.actiontech.dble.net.NIOProcessor;
import com.actiontech.dble.net.mysql.AuthPacket;
import com.actiontech.dble.net.mysql.MySQLPacket;
import com.actiontech.dble.net.mysql.QuitPacket;
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
        }

        AuthPacket auth = new AuthPacket();
        auth.read(data);

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

        // check degrade
        if (isDegrade(auth.getUser())) {
            failure(ErrorCode.ER_ACCESS_DENIED_ERROR, "Access denied for user '" + auth.getUser() + "', because service be degraded ");
            return;
        }

        // check dataHost without writeHost flag
        if (DbleServer.getInstance().getConfig().isDataHostWithoutWR() && !(this instanceof ManagerAuthenticator)) {
            failure(ErrorCode.ER_ACCESS_DENIED_ERROR, "Access denied for user '" + auth.getUser() + "', because there have dataHost without writeHost ");
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
                success(auth);
        }
    }

    //frontend connection reached the user threshold. service degrade
    protected boolean isDegrade(String user) {

        int benchmark = source.getPrivileges().getBenchmark(user);
        if (benchmark > 0) {

            int forntedsLength = 0;
            NIOProcessor[] processors = DbleServer.getInstance().getProcessors();
            for (NIOProcessor p : processors) {
                forntedsLength += p.getForntedsLength();
            }

            if (forntedsLength >= benchmark) {
                return true;
            }
        }

        return false;
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
            LOGGER.warn(source.toString(), e);
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

    protected NIOHandler successCommendHander() {
        return new FrontendCommandHandler(source);
    }

    protected void success(AuthPacket auth) {
        source.setAuthenticated(true);
        source.setUser(auth.getUser());
        source.setSchema(auth.getDatabase());
        source.setCharsetIndex(auth.getCharsetIndex());
        source.setHandler(successCommendHander());

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

    protected void failure(int errno, String info) {
        LOGGER.error(source.toString() + info);
        source.writeErrMessage((byte) 2, errno, info);
    }

}
