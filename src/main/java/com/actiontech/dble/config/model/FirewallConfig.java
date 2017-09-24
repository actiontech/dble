/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.config.model;

import com.alibaba.druid.wall.WallConfig;
import com.alibaba.druid.wall.WallProvider;
import com.alibaba.druid.wall.spi.MySqlWallProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * FirewallConfig
 *
 * @author songwie
 * @author zhuam
 */
public final class FirewallConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(FirewallConfig.class);

    private Map<String, List<UserConfig>> whitehost;
    private boolean blackListCheck = false;

    private WallConfig wallConfig = new WallConfig();

    private WallProvider provider;

    public FirewallConfig() {
    }

    public void init() {
        if (blackListCheck) {
            provider = new MySqlWallProvider(wallConfig);
            provider.setBlackListEnable(true);
        }
    }

    public Map<String, List<UserConfig>> getWhitehost() {
        return this.whitehost;
    }

    public void setWhitehost(Map<String, List<UserConfig>> whitehost) {
        this.whitehost = whitehost;
    }

    public boolean addWhitehost(String host, List<UserConfig> users) {
        if (existsHost(host)) {
            return false;
        } else {
            this.whitehost.put(host, users);
            return true;
        }
    }

    public WallProvider getProvider() {
        return provider;
    }

    public boolean existsHost(String host) {
        return this.whitehost != null && whitehost.get(host) != null;
    }

    public void setWallConfig(WallConfig wallConfig) {
        this.wallConfig = wallConfig;

    }

    public boolean isBlackListCheck() {
        return this.blackListCheck;
    }

    public void setBlackListCheck(boolean blackListCheck) {
        this.blackListCheck = blackListCheck;
    }

}
