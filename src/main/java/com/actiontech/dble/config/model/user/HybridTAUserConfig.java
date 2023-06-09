package com.actiontech.dble.config.model.user;

import com.alibaba.druid.wall.WallProvider;

import java.util.Set;

public class HybridTAUserConfig extends ShardingUserConfig {

    public HybridTAUserConfig(UserConfig user, String tenant, WallProvider blacklist, boolean readOnly, Set<String> schemas, UserPrivilegesConfig privilegesConfig) {
        super(user, tenant, blacklist, readOnly, schemas, privilegesConfig);
    }
}
