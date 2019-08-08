package com.actiontech.dble.meta;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * Created by szf on 2019/7/16.
 */
public class ReloadLogHelper {
    private static final Logger LOGGER = LoggerFactory.getLogger("META-REACQUIRE");
    public final ReloadStatus reload;
    private final boolean isReload;

    public ReloadLogHelper(boolean isReload) {
        this.isReload = isReload;
        if (isReload) {
            reload = ReloadManager.getReloadInstance().getStatus();
        } else {
            reload = null;
        }
    }

    public static void infoList(String message, Logger logger, Set<String> keySet) {
        StringBuffer sb = new StringBuffer("");
        if (keySet != null) {
            for (String x : keySet) {
                sb.append(x);
            }
        }
        if (ReloadManager.getReloadInstance().getStatus() == null) {
            logger.info(message + " " + sb.toString());
        } else {
            logger.info(ReloadManager.getReloadInstance().getStatus().getLogStage() + message + " " + sb.toString());
        }
    }

    public void infoList(String message, Set<String> keySet) {
        StringBuffer sb = new StringBuffer("");
        if (keySet != null) {
            for (String x : keySet) {
                sb.append(x);
            }
        }
        LOGGER.info(getStage() + message + " " + sb.toString());
    }

    public static void info(String message, Logger logger) {
        if (ReloadManager.getReloadInstance().getStatus() != null) {
            logger.info(ReloadManager.getReloadInstance().getStatus().getLogStage() + message);
        } else {
            logger.info(message);
        }
    }

    public void info(String message) {
        LOGGER.info(getStage() + message);
    }

    public static void warn(String message, Logger logger) {
        if (ReloadManager.getReloadInstance().getStatus() != null) {
            logger.info(ReloadManager.getReloadInstance().getStatus().getLogStage() + message);
        } else {
            logger.info(message);
        }
    }

    public static void warn(String message, Throwable var2, Logger logger) {
        if (ReloadManager.getReloadInstance().getStatus() != null) {
            logger.info(ReloadManager.getReloadInstance().getStatus().getLogStage() + message, var2);
        } else {
            logger.info(message, var2);
        }
    }

    public void warn(String message) {
        LOGGER.warn(getStage() + message);
    }

    public void warn(String message, Throwable var2) {
        LOGGER.warn(getStage() + message, var2);
    }



    private String getStage() {
        return reload == null ? "" : reload.getLogStage();
    }

    public boolean isReload() {
        return isReload;
    }

}
