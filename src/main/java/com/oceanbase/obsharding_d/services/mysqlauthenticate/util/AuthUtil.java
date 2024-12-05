/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.mysqlauthenticate.util;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.config.model.user.ManagerUserConfig;
import com.oceanbase.obsharding_d.config.model.user.UserConfig;
import com.oceanbase.obsharding_d.config.model.user.UserName;
import com.oceanbase.obsharding_d.net.connection.FrontendConnection;
import com.oceanbase.obsharding_d.net.mysql.AuthPacket;
import com.oceanbase.obsharding_d.net.mysql.ChangeUserPacket;
import com.oceanbase.obsharding_d.net.service.AuthResultInfo;
import com.oceanbase.obsharding_d.services.mysqlauthenticate.PluginName;
import com.oceanbase.obsharding_d.services.mysqlauthenticate.SecurityUtil;
import com.oceanbase.obsharding_d.singleton.FrontendUserManager;
import com.oceanbase.obsharding_d.util.IPAddressUtil;

import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.Set;

public final class AuthUtil {
    private AuthUtil() {
    }

    public static AuthResultInfo auth(FrontendConnection fconn, byte[] seed, PluginName plugin, AuthPacket authPacket) {
        UserName user = new UserName(authPacket.getUser(), authPacket.getTenant());
        UserConfig userConfig = OBsharding_DServer.getInstance().getConfig().getUsers().get(user);
        if (userConfig == null) {
            return new AuthResultInfo("Access denied for user '" + user.getFullName() + "' with host '" + fconn.getHost() + "'");
        }
        //normal user login into manager port
        if (fconn.isManager() && !(userConfig instanceof ManagerUserConfig)) {
            return new AuthResultInfo("Access denied for user '" + user.getFullName() + "'");
        } else if (!fconn.isManager() && userConfig instanceof ManagerUserConfig) {
            //manager user login into server port
            return new AuthResultInfo("Access denied for manager user '" + user.getFullName() + "'");
        }
        if (!checkWhiteIPs(fconn.getHost(), userConfig.getWhiteIPs())) {
            return new AuthResultInfo("Access denied for user '" + user.getFullName() + "' with host '" + fconn.getHost() + "'");
        }
        // check password
        if (!checkPassword(seed, authPacket.getPassword(), userConfig.getPassword(), plugin)) {
            return new AuthResultInfo("Access denied for user '" + user.getFullName() + "', because password is incorrect");
        }
        if (!OBsharding_DServer.getInstance().getConfig().isFullyConfigured() && !fconn.isManager()) {
            return new AuthResultInfo("Access denied for user '" + user.getFullName() + "', because there are some empty dbGroup/fake dbInstance");
        }
        // check schema
        final String schema = authPacket.getDatabase();
        switch (userConfig.checkSchema(schema)) {
            case ErrorCode.ER_BAD_DB_ERROR:
                return new AuthResultInfo("Unknown database '" + schema + "'");
            case ErrorCode.ER_DBACCESS_DENIED_ERROR:
                return new AuthResultInfo("Access denied for user '" + user.getFullName() + "' to database '" + schema + "'");
            default:
                break;
        }
        //check max connection
        switch (FrontendUserManager.getInstance().maxConnectionCheck(user, userConfig.getMaxCon(), fconn.isManager())) {
            case SERVER_MAX:
                return new AuthResultInfo("Access denied for user '" + user.getFullName() + "',too many connections for OBsharding-D server");
            case USER_MAX:
                return new AuthResultInfo("Access denied for user '" + user.getFullName() + "',too many connections for this user");
            default:
                break;
        }
        return new AuthResultInfo(null, authPacket, user, userConfig);
    }

    public static AuthResultInfo auth(FrontendConnection fconn, byte[] seed, PluginName plugin, ChangeUserPacket changeUserPacket) {
        UserName user = new UserName(changeUserPacket.getUser(), changeUserPacket.getTenant());
        UserConfig userConfig = OBsharding_DServer.getInstance().getConfig().getUsers().get(user);
        if (userConfig == null) {
            return new AuthResultInfo("Access denied for user '" + user.getFullName() + "' with host '" + fconn.getHost() + "'");
        }
        //normal user login into manager port
        if (fconn.isManager() && !(userConfig instanceof ManagerUserConfig)) {
            return new AuthResultInfo("Access denied for user '" + user.getFullName() + "'");
        } else if (!fconn.isManager() && userConfig instanceof ManagerUserConfig) {
            //manager user login into server port
            return new AuthResultInfo("Access denied for manager user '" + user.getFullName() + "'");
        }
        if (!checkWhiteIPs(fconn.getHost(), userConfig.getWhiteIPs())) {
            return new AuthResultInfo("Access denied for user '" + user.getFullName() + "' with host '" + fconn.getHost() + "'");
        }
        // check password
        if (!checkPassword(seed, changeUserPacket.getPassword(), userConfig.getPassword(), plugin)) {
            return new AuthResultInfo("Access denied for user '" + user.getFullName() + "', because password is incorrect");
        }
        // check schema
        final String schema = changeUserPacket.getDatabase();
        switch (userConfig.checkSchema(schema)) {
            case ErrorCode.ER_BAD_DB_ERROR:
                return new AuthResultInfo("Unknown database '" + schema + "'");
            case ErrorCode.ER_DBACCESS_DENIED_ERROR:
                return new AuthResultInfo("Access denied for user '" + user.getFullName() + "' to database '" + schema + "'");
            default:
                break;
        }

        return new AuthResultInfo(null, null, user, userConfig);
    }

    private static boolean checkWhiteIPs(String host, Set<String> whiteIPs) {
        // whether to check
        if (null == whiteIPs || whiteIPs.size() == 0) {
            return true;
        }
        return whiteIPs.stream().anyMatch(e -> {
            try {
                return IPAddressUtil.match(host, e);
            } catch (UnknownHostException unknownHostException) {
                return false;
            }
        });
    }

    private static boolean checkPassword(byte[] seed, byte[] password, String pass, PluginName name) {
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
            switch (name) {
                case mysql_native_password:
                    encryptPass = SecurityUtil.scramble411(pass.getBytes(), seed);
                    break;
                case caching_sha2_password:
                    encryptPass = SecurityUtil.scramble256(pass.getBytes(), seed);
                    break;
                default:
                    throw new NoSuchAlgorithmException();
            }
        } catch (NoSuchAlgorithmException e) {
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

}
