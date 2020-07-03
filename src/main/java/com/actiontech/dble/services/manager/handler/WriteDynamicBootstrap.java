/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.handler;

import com.actiontech.dble.config.loader.SystemConfigLoader;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.util.PropertiesUtil;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

public final class WriteDynamicBootstrap {
    private static final WriteDynamicBootstrap INSTANCE = new WriteDynamicBootstrap();

    private WriteDynamicBootstrap() {
    }

    public static WriteDynamicBootstrap getInstance() {
        return INSTANCE;
    }

    public synchronized void changeValue(List<Pair<String, String>> kvs) throws IOException {
        Properties pros = SystemConfigLoader.readBootStrapDynamicConf();
        for (Pair<String, String> kv : kvs) {
            pros.setProperty(kv.getKey(), kv.getValue());
        }
        PropertiesUtil.storeProperties(pros, SystemConfigLoader.BOOT_STRAP_DYNAMIC_FILE_NAME);
    }

    public synchronized void changeValue(String key, String value) throws IOException {
        Properties pros = SystemConfigLoader.readBootStrapDynamicConf();
        pros.setProperty(key, value);
        PropertiesUtil.storeProperties(pros, SystemConfigLoader.BOOT_STRAP_DYNAMIC_FILE_NAME);
    }
}
