/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d;

import com.oceanbase.obsharding_d.cluster.ClusterController;
import com.oceanbase.obsharding_d.config.Versions;
import com.oceanbase.obsharding_d.config.loader.SystemConfigLoader;
import com.oceanbase.obsharding_d.config.util.StartProblemReporter;
import com.oceanbase.obsharding_d.singleton.CustomMySQLHa;
import com.oceanbase.obsharding_d.singleton.OnlineStatus;
import com.oceanbase.obsharding_d.util.CheckConfigurationUtil;
import com.alibaba.druid.sql.SQLUtils;

public final class OBsharding_DStartup {
    static {
        SQLUtils.DEFAULT_FORMAT_OPTION.setPrettyFormat(false);
    }

    private OBsharding_DStartup() {
    }

    public static void main(String[] args) {
        try {
            CheckConfigurationUtil.checkConfiguration();
            ClusterController.loadClusterProperties();
            // load system properties
            SystemConfigLoader.initSystemConfig();
            // load system other properties
            SystemConfigLoader.verifyOtherParam();
            if (StartProblemReporter.getInstance().getErrorConfigs().size() > 0) {
                for (String errInfo : StartProblemReporter.getInstance().getErrorConfigs()) {
                    System.out.println(errInfo);
                }
                System.exit(-1);
            }
            if (!ClusterController.tryServerStartDuringInitClusterData()) {
                initClusterAndServerStart();
            }
            System.out.println("Server startup successfully. OBsharding-D version is [" + new String(Versions.getServerVersion()) + "]. Please see logs in logs/OBsharding-D.log");
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    /**
     * may not public ,now no better solution
     *
     * @throws Exception
     */
    public static void initClusterAndServerStart() throws Exception {
        ClusterController.init();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Server execute ShutdownHook.");
            OnlineStatus.getInstance().shutdownClear();
            CustomMySQLHa.getInstance().stop(true);
        }));
        // startup
        OBsharding_DServer.getInstance().startup();
    }
}
