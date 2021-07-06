/*
 * Copyright (C) 2016-2021 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble;

import com.actiontech.dble.cluster.ClusterController;
import com.actiontech.dble.config.Versions;
import com.actiontech.dble.config.loader.SystemConfigLoader;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.util.StartProblemReporter;
import com.actiontech.dble.singleton.CustomMySQLHa;
import com.actiontech.dble.singleton.OnlineStatus;
import com.actiontech.dble.util.CheckConfigurationUtil;

public final class DbleStartup {
    private DbleStartup() {
    }

    public static void main(String[] args) {
        try {
            CheckConfigurationUtil.checkConfiguration();
            ClusterController.loadClusterProperties();
            //lod system properties
            SystemConfigLoader.initSystemConfig();
            if (SystemConfig.getInstance().getInstanceName() == null) {
                StartProblemReporter.getInstance().addError("You must config instanceName in bootstrap.cnf and make sure it is an unique key for cluster");
            }
            String home = SystemConfig.getInstance().getHomePath();
            if (home == null) {
                StartProblemReporter.getInstance().addError("homePath is not set.");
            }
            if (StartProblemReporter.getInstance().getErrorConfigs().size() > 0) {
                for (String errInfo : StartProblemReporter.getInstance().getErrorConfigs()) {
                    System.out.println(errInfo);
                }
                System.exit(-1);
            }
            if (!ClusterController.tryServerStartDuringInitClusterData()) {
                initClusterAndServerStart();
            }
            System.out.println("Server startup successfully. dble version is [" + new String(Versions.getServerVersion()) + "]. Please see logs in logs/dble.log");
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    /**
     * may not public ,now no better solution
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
        DbleServer.getInstance().startup();
    }
}
