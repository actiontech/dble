/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.config;

import com.oceanbase.obsharding_d.cluster.ClusterController;
import com.oceanbase.obsharding_d.cluster.general.impl.UcoreSender;
import com.oceanbase.obsharding_d.util.ResourceUtil;
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
