/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble;


import com.actiontech.dble.cluster.ClusterController;
import com.actiontech.dble.config.Versions;
import com.actiontech.dble.config.loader.SystemConfigLoader;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.manager.handler.ShowServerLog;
import com.actiontech.dble.singleton.CustomMySQLHa;
import com.actiontech.dble.singleton.OnlineStatus;

public final class DbleStartup {
    private DbleStartup() {
    }

    public static void main(String[] args) {
        try {
            ClusterController.loadClusterProperties();
            //lod system properties
            SystemConfigLoader.initSystemConfig();
            if (SystemConfig.getInstance().getInstanceName() == null) {
                String msg = "You must config instanceName in bootstrap.cnf and make sure it is an unique key for cluster";
                System.out.println(msg);
                System.exit(-1);
            }
            ClusterController.init();
            String home = SystemConfig.getInstance().getHomePath();
            if (home == null) {
                System.out.println("homePath is not set.");
                System.exit(-1);
            }
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    System.out.println("Server execute ShutdownHook.");
                    OnlineStatus.getInstance().shutdownClear();
                    CustomMySQLHa.getInstance().stop(true);
                }
            });
            // startup
            DbleServer.getInstance().startup();
            System.out.println("Server startup successfully. dble version is [" + new String(Versions.getServerVersion()) + "]. Please see logs in logs/" + ShowServerLog.DEFAULT_LOGFILE);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}
