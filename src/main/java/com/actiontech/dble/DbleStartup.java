/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble;


import com.actiontech.dble.cluster.ClusterController;
import com.actiontech.dble.config.Versions;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.manager.handler.ShowServerLog;

/**
 * @author mycat
 */
public final class DbleStartup {
    private DbleStartup() {
    }

    public static void main(String[] args) {
        ClusterController.init();
        try {
            String home = SystemConfig.getHomePath();
            if (home == null) {
                System.out.println(SystemConfig.SYS_HOME + "  is not set.");
                System.exit(-1);
            }
            // startup
            DbleServer.getInstance().startup();
            System.out.println("Server startup successfully. dble version is [" + new String(Versions.getServerVersion()) + "]. Please see logs in logs/" + ShowServerLog.DEFAULT_LOGFILE);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}
