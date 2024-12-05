/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.singleton;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.oceanbase.obsharding_d.util.IOUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

// only work for linux
public final class CustomMySQLHa {
    private static final Logger LOGGER = LoggerFactory.getLogger(CustomMySQLHa.class);
    private static final CustomMySQLHa INSTANCE = new CustomMySQLHa();
    /**
     * notice:
     * 1. this {@code process} variable is null before first init. once this set to notNull. It won't set to be null again.
     * 2. this {@code process} variable isn't thread safe.
     */
    private volatile Process process;

    private CustomMySQLHa() {
    }

    public static CustomMySQLHa getInstance() {
        return INSTANCE;
    }

    // return null if success
    public synchronized String start() {
        if (SystemConfig.getInstance().isUseOuterHa()) {
            String msg = "You use OuterHa or Cluster, please use the third party HA Component";
            LOGGER.debug(msg);
            return msg;
        }
        if (isProcessAlive()) {
            return "python process exists";
        }
        String exe = "python3";
        String file = SystemConfig.getInstance().getHomePath() + File.separatorChar + "bin" + File.separatorChar + "custom_mysql_ha.py";
        String[] cmdArr = new String[]{exe, file};
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("a new  process `" + exe + " " + file + "` will be execute ");
        }
        try {
            process = Runtime.getRuntime().exec(cmdArr);
            synchronized (process) {
                /*
                wait for a while to catch most of bootstrap errors.
                 */
                final boolean nonTimeout = process.waitFor(200, TimeUnit.MILLISECONDS);
                if (nonTimeout) {
                    String msg;
                    final int exitValue = process.exitValue();
                    if (exitValue != 0) {
                        msg = "starting simple_ha_switch error with exitCode:" + exitValue;
                        LOGGER.warn("{},script error log :`{}`", msg, IOUtil.convertStreamToString(process.getErrorStream(), Charset.defaultCharset()));
                        msg += ", view logs for more details";
                    } else {
                        msg = "starting simple_ha_switch exit with exitCode:" + exitValue;
                        LOGGER.warn(msg);
                    }
                    return msg;
                }
            }
            OBsharding_DServer.getInstance().getComplexQueryExecutor().execute(() -> {
                try {
                    synchronized (process) {
                        /*
                        waitFor() will call process.wait()
                        wait() will release the synchronized lock of 'process'.
                         */
                        final int exitValue = process.waitFor();

                        if (exitValue != 0) {
                            LOGGER.warn("starting simple_ha_switch error with exitCode:{},script error log is `{}`", exitValue, IOUtil.convertStreamToString(process.getErrorStream(), Charset.defaultCharset()));
                        }
                    }

                } catch (InterruptedException | IOException e) {
                    String msg = "starting simple_ha_switch occurred IOException";
                    LOGGER.warn(msg, e);
                }

            });

        } catch (IOException | InterruptedException e) {
            String msg = "starting simple_ha_switch occurred " + e.getClass().getSimpleName();
            LOGGER.warn(msg, e);
            msg += ", view logs for more details";
            return msg;
        }
        return null;
    }

    // return null if success
    public synchronized String stop(boolean byHook) {
        if (SystemConfig.getInstance().isUseOuterHa()) {
            String msg = "You use OuterHa or Cluster, please use the third party HA Component";
            if (byHook) {
                System.out.println("You use OuterHa or Cluster, no need to clean up ha process");
            } else if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(msg);
            }
            return msg;
        }
        if (!isProcessAlive()) {
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
            synchronized (process) {
                //Maybe this destroy operation is redundant. Not 100% sure.
                process.destroyForcibly();
                process.waitFor();
            }
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
        if (process != null) {
            synchronized (process) {
                return process.isAlive();
            }
        }
        return false;
    }

    private long getPidOfLinux(boolean byHook) {
        long pid = -1;

        try {
            if (process != null) {
                synchronized (process) {
                    if (process.getClass().getName().equals("java.lang.UNIXProcess")) {
                        Field f = process.getClass().getDeclaredField("pid");
                        f.setAccessible(true);
                        pid = f.getLong(process);
                        f.setAccessible(false);
                    }
                }
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
