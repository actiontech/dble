/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble;

import com.actiontech.dble.config.ConfigInitializer;
import org.junit.Test;

/**
 * @author mycat
 */
public class ConfigInitializerTest {
    @Test
    public void testConfigLoader() {
        new ConfigInitializer(true);
    }
}