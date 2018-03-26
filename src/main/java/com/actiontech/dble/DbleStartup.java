/*
* Copyright (C) 2016-2018 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble;


import com.actiontech.dble.cluster.ClusterController;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.log.alarm.AlarmCode;
import com.actiontech.dble.manager.handler.ShowServerLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author mycat
 */
public final class DbleStartup {
    private DbleStartup() {
    }

    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static final Logger LOGGER = LoggerFactory.getLogger(DbleStartup.class);
    private static boolean initZKend = false;

    public static void main(String[] args) {
        //use zk ?
        /*ZkConfig.getInstance().initZk();
        UcoreConfig.getInstance().initUcore();*/
        ClusterController.init();
        try {
            String home = SystemConfig.getHomePath();
            if (home == null) {
                System.out.println(SystemConfig.SYS_HOME + "  is not set.");
                System.exit(-1);
            }
            // init

            DbleServer server = DbleServer.getInstance();
            initZKend = true;
            server.beforeStart();

            // startup
            server.startup();
            System.out.println("Server startup successfully. see logs in logs/" + ShowServerLog.DEFAULT_LOGFILE);

        } catch (Exception e) {
            SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
            LOGGER.error(AlarmCode.CORE_ERROR +
                    sdf.format(new Date()) + " startup error", e);
            System.exit(-1);
        }
    }


    public static boolean isInitZKend() {
        return initZKend;
    }

    public static void setInitZKend(boolean initZKend) {
        DbleStartup.initZKend = initZKend;
    }
}
