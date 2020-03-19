/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.config;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.model.FirewallConfig;
import com.actiontech.dble.config.model.UserConfig;
import com.actiontech.dble.config.model.UserPrivilegesConfig;
import com.actiontech.dble.net.handler.FrontendPrivileges;
import com.actiontech.dble.server.ServerConnection;
import com.alibaba.druid.wall.WallCheckResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author mycat
 */
public class ServerPrivileges implements FrontendPrivileges {
    private static ServerPrivileges instance = new ServerPrivileges();

    private static final Logger LOGGER = LoggerFactory.getLogger(ServerPrivileges.class);

    public static ServerPrivileges instance() {
        return instance;
    }

    protected ServerPrivileges() {
        super();
    }

    @Override
    public boolean schemaExists(String schema) {
        return DbleServer.getInstance().getConfig().getSchemas().containsKey(schema);
    }

    @Override
    public boolean userExists(String user, String host) {
        return checkFirewallWhiteHostPolicy(user, host);
    }

    @Override
    public String getPassword(String user) {
        ServerConfig conf = DbleServer.getInstance().getConfig();
        UserConfig uc = conf.getUsers().get(user);
        if (uc != null) {
            return uc.getPassword();
        } else {
            return null;
        }
    }

    @Override
    public Set<String> getUserSchemas(String user) {
        ServerConfig conf = DbleServer.getInstance().getConfig();
        UserConfig uc = conf.getUsers().get(user);
        if (uc != null) {
            return uc.getSchemas();
        } else {
            return null;
        }
    }

    @Override
    public boolean isReadOnly(String user) {
        ServerConfig conf = DbleServer.getInstance().getConfig();
        UserConfig uc = conf.getUsers().get(user);
        boolean result = false;
        if (uc != null) {
            result = uc.isReadOnly();
        }
        return result;
    }

    @Override
    public int getMaxCon(String user) {
        ServerConfig conf = DbleServer.getInstance().getConfig();
        UserConfig uc = conf.getUsers().get(user);
        if (uc != null) {
            return uc.getMaxCon();
        } else {
            return 0;
        }
    }

    protected boolean checkManagerPrivilege(String user) {
        //  normal user don't need manager privilege
        return true;
    }

    @Override
    public boolean checkFirewallWhiteHostPolicy(String user, String host) {
        if (!checkManagerPrivilege(user)) {
            // normal user try to login by manager port
            return false;
        }
        boolean isPassed = false;
        ServerConfig config = DbleServer.getInstance().getConfig();
        FirewallConfig firewallConfig = config.getFirewall();
        Map<String, List<UserConfig>> whiteHost = firewallConfig.getWhitehost();
        if (whiteHost == null || whiteHost.size() == 0) {
            Map<String, UserConfig> users = config.getUsers();
            isPassed = users.containsKey(user);
        } else {
            List<UserConfig> list = whiteHost.get(host);
            if (list != null) {
                for (UserConfig userConfig : list) {
                    if (userConfig.getName().equals(user)) {
                        isPassed = true;
                        break;
                    }
                }
            }
        }

        if (!isPassed) {
            LOGGER.warn("checkFirewallWhiteHostPolicy for [host=" + host + ",user=" + user + "],but not passed");
            return false;
        }
        return true;
    }


    /**
     * @see <a href="https://github.com/alibaba/druid/wiki/%E9%85%8D%E7%BD%AE-wallfilter">wallfilter config guide</a>
     */
    @Override
    public boolean checkFirewallSQLPolicy(String user, String sql) {
        if (isManagerUser(user)) {
            // manager User will ignore firewall blacklist
            return true;
        }
        boolean isPassed = true;
        FirewallConfig firewallConfig = DbleServer.getInstance().getConfig().getFirewall();
        if (firewallConfig != null && firewallConfig.isBlackListCheck()) {
            WallCheckResult result = firewallConfig.getProvider().check(sql);
            if (!result.getViolations().isEmpty()) {
                isPassed = false;
                LOGGER.warn("Firewall to intercept the '" + user + "' unsafe SQL , errMsg:" +
                        result.getViolations().get(0).getMessage() + " \r\n " + sql);
            }
        }
        return isPassed;
    }

    @Override
    public boolean isManagerUser(String user) {
        ServerConfig conf = DbleServer.getInstance().getConfig();
        UserConfig uc = conf.getUsers().get(user);
        return uc != null && uc.isManager();
    }

    public enum CheckType {
        INSERT, UPDATE, SELECT, DELETE
    }

    // check SQL Privilege
    public static boolean checkPrivilege(ServerConnection source, String schema, String tableName, CheckType chekcType) {
        UserConfig userConfig = DbleServer.getInstance().getConfig().getUsers().get(source.getUser());
        if (userConfig == null) {
            return true;
        }
        UserPrivilegesConfig userPrivilege = userConfig.getPrivilegesConfig();
        if (userPrivilege == null || !userPrivilege.isCheck()) {
            return true;
        }
        UserPrivilegesConfig.SchemaPrivilege schemaPrivilege = userPrivilege.getSchemaPrivilege(schema);
        if (schemaPrivilege == null) {
            return true;
        }
        UserPrivilegesConfig.TablePrivilege tablePrivilege = schemaPrivilege.getTablePrivilege(tableName);
        if (tablePrivilege == null && schemaPrivilege.getDml().length == 0) {
            return true;
        }
        int index = -1;
        if (chekcType == CheckType.INSERT) {
            index = 0;
        } else if (chekcType == CheckType.UPDATE) {
            index = 1;
        } else if (chekcType == CheckType.SELECT) {
            index = 2;
        } else if (chekcType == CheckType.DELETE) {
            index = 3;
        }
        if (tablePrivilege != null) {
            return tablePrivilege.getDml()[index] > 0;
        } else {
            return schemaPrivilege.getDml()[index] > 0;
        }
    }
}
