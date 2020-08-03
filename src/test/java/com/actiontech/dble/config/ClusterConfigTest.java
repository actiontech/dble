/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config;

import com.actiontech.dble.cluster.ClusterController;
import com.actiontech.dble.cluster.general.impl.UcoreSender;
import com.actiontech.dble.util.ResourceUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

public class ClusterConfigTest {
    @Test
    public void ucoreClusterConfigStore() throws InvocationTargetException, IllegalAccessException, IOException {
        Properties pros = ClusterController.readClusterProperties();
        ClusterController.loadClusterProperties();
        UcoreSender sender = new UcoreSender();
        sender.setIp("127.0.0.1");
        Properties pros2 = ClusterController.readClusterProperties();
        Assert.assertTrue(pros.equals(pros2));
    }
}
