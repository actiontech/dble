/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.singleton;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.model.SystemConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;

// only work for linux
public final class CustomMySQLHa {
    private static final Logger LOGGER = LoggerFactory.getLogger(CustomMySQLHa.class);
    private static final CustomMySQLHa INSTANCE = new CustomMySQLHa();
    private Process process;

    private CustomMySQLHa() {
    }

    public static CustomMySQLHa getInstance() {
        return INSTANCE;
    }

    // return null if success
    public String start() {
        if (DbleServer.getInstance().getConfig().getSystem().isUseOuterHa() || ClusterGeneralConfig.getInstance().isUseCluster()) {
            String msg = "You use OuterHa or Cluster, please use the third party HA Component";
            LOGGER.debug(msg);
            return msg;
        }
        if (process != null && process.isAlive()) {
            return "python process exists";
        }
        String exe = "python";
        String file = SystemConfig.getHomePath() + File.separatorChar + "bin" + File.separatorChar + "custom_mysql_ha.py";
        String[] cmdArr = new String[]{exe, file};
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("a new  process `" + exe + " " + file + "` will be execute ");
        }
        try {
            process = Runtime.getRuntime().exec(cmdArr);
            if (!process.isAlive()) {
                String msg = "starting simple_ha_switch error " + process.exitValue();
                LOGGER.warn(msg);
                return msg;
            }
        } catch (IOException e) {
            String msg = "starting simple_ha_switch occurred IOException";
            LOGGER.warn(msg, e);
            return msg;
        }
        return null;
    }

    // return null if success
    public String stop(boolean byHook) {
        if (DbleServer.getInstance().getConfig().getSystem().isUseOuterHa() || ClusterGeneralConfig.getInstance().isUseCluster()) {
            String msg = "You use OuterHa or Cluster, please use the third party HA Component";
            if (byHook) {
                System.out.println(msg);
            } else if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(msg);
            }
            return msg;
        }
        if (process == null || !process.isAlive()) {
            String msg = "python process does not exists";
            if (byHook) {
                System.out.println(msg);
            } else {
                LOGGER.warn(msg);
            }
            return msg;
        }
        long pid = getPidOfLinux(byHook);
        if (pid == -1) {
            return "get Pid Of Python Script failed";
        }
        String exe = "kill";
        String command = "-9";
        String[] cmdArr = new String[]{exe, command, Long.toString(pid)};
        try {
            Process killProcess = Runtime.getRuntime().exec(cmdArr);
            int returnCode = killProcess.waitFor();
            String msg = "the result of `kill -9 " + pid + "` is " + returnCode;
            if (byHook) {
                System.out.println(msg);
            } else {
                LOGGER.debug(msg);
            }
            process = null;
            if (returnCode != 0) {
                return msg;
            } else {
                return null;
            }
        } catch (IOException e) {
            String msg = "killing simple_ha_switch occurred IOException";
            if (byHook) {
                System.out.println(msg + e);
            } else {
                LOGGER.warn(msg, e);
            }
            return msg;
        } catch (InterruptedException e) {
            String msg = "killing simple_ha_switch was Interrupted";
            if (byHook) {
                System.out.println(msg + e);
            } else {
                LOGGER.warn(msg, e);
            }
            return msg;
        }
    }

    public boolean isProcessAlive() {
        return process != null && process.isAlive();
    }

    private long getPidOfLinux(boolean byHook) {
        long pid = -1;

        try {
            if (process.getClass().getName().equals("java.lang.UNIXProcess")) {
                Field f = process.getClass().getDeclaredField("pid");
                f.setAccessible(true);
                pid = f.getLong(process);
                f.setAccessible(false);
            }
        } catch (Exception e) {
            if (byHook) {
                System.out.println("getPidOfLinux failed:" + e);
            } else {
                LOGGER.warn("getPidOfLinux failed:", e);
            }
            pid = -1;
        }
        if (byHook) {
            System.out.println("python pid is " + pid);
        } else if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("python pid is " + pid);
        }

        return pid;
    }
}
