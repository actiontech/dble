/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.util;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.SecurityUtil;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.singleton.FrontendUserManager;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.FrontendConnection;
import com.actiontech.dble.net.handler.FrontendPrivileges;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.NoSuchAlgorithmException;
import java.util.Set;

public final class AuthUtil {
    private AuthUtil() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(AuthUtil.class);

    public static boolean authority(final FrontendConnection source, String user, byte[] pwd, String schema, boolean isManager) {
        if (!checkUser(source, user)) {
            failure(source, ErrorCode.ER_ACCESS_DENIED_ERROR, "Access denied for user '" + user + "' with host '" + source.getHost() + "'");
            return false;
        }

        // check password
        if (!checkPassword(source, pwd, user)) {
            failure(source, ErrorCode.ER_ACCESS_DENIED_ERROR, "Access denied for user '" + user + "', because password is error ");
            return false;
        }

        // check dataHost without writeHost flag
        if (DbleServer.getInstance().getConfig().isDataHostWithoutWR() && !isManager) {
            failure(source, ErrorCode.ER_ACCESS_DENIED_ERROR, "Access denied for user '" + user + "', because there are some dataHost is empty ");
            return false;
        }

        // check schema
        switch (checkSchema(source, schema, user)) {
            case ErrorCode.ER_BAD_DB_ERROR:
                failure(source, ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + schema + "'");
                return false;
            case ErrorCode.ER_DBACCESS_DENIED_ERROR:
                String s = "Access denied for user '" + user + "' to database '" + schema + "'";
                failure(source, ErrorCode.ER_DBACCESS_DENIED_ERROR, s);
                return false;
            default:
                break;
        }

        //check maxconnection
        switch (FrontendUserManager.getInstance().maxConnectionCheck(user, source.getPrivileges().getMaxCon(user), (source instanceof ManagerConnection))) {
            case SERVER_MAX:
                String s = "Access denied for user '" + user + "',too many connections for dble server";
                failure(source, ErrorCode.ER_ACCESS_DENIED_ERROR, s);
                return false;
            case USER_MAX:
                String s1 = "Access denied for user '" + user + "',too many connections for this user";
                failure(source, ErrorCode.ER_ACCESS_DENIED_ERROR, s1);
                return false;
            default:
                break;
        }
        return true;
    }


    private static boolean checkUser(final FrontendConnection source, String user) {
        return source.getPrivileges().userExists(user, source.getHost());
    }

    private static boolean checkPassword(final FrontendConnection source, byte[] password, String user) {
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

    private static int checkSchema(final FrontendConnection source, String schema, String user) {
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

    private static void failure(final FrontendConnection source, int errNo, String info) {
        LOGGER.info(source.toString() + info);
        source.writeErrMessage((byte) 2, errNo, info);
    }
}
