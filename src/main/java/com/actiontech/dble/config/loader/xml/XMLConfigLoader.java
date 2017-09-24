/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.config.loader.xml;

import com.actiontech.dble.config.model.FirewallConfig;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.UserConfig;

import java.util.Map;

/**
 * @author mycat
 */
public class XMLConfigLoader {

    private final SystemConfig system;
    /**
     * unmodifiable
     */
    private final Map<String, UserConfig> users;
    private final FirewallConfig firewall;

    public XMLConfigLoader() {
        XMLServerLoader serverLoader = new XMLServerLoader();
        this.system = serverLoader.getSystem();
        this.users = serverLoader.getUsers();
        this.firewall = serverLoader.getFirewall();
    }

    public FirewallConfig getFirewallConfig() {
        return firewall;
    }

    public Map<String, UserConfig> getUserConfigs() {
        return users;
    }

    public SystemConfig getSystemConfig() {
        return system;
    }


}
