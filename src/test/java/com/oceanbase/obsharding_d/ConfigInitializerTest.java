/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d;

import com.oceanbase.obsharding_d.config.ConfigInitializer;
import org.junit.Test;

/**
 * @author mycat
 */
public class ConfigInitializerTest {
    @Test
    public void testConfigLoader() {
        new ConfigInitializer();
    }
}