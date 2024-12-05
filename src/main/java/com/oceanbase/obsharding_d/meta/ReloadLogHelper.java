/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.meta;

import com.oceanbase.obsharding_d.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * Created by szf on 2019/7/16.
 */
public class ReloadLogHelper {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReloadLogHelper.class);
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

    public void info(String message) {
        LOGGER.info(getStage() + message);
    }

    public void infoList(String message, Set<String> keySet) {
        String sb = keySet == null ? "" : StringUtil.join(keySet, ",");
        LOGGER.info(getStage() + message + " " + sb);
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


    // ========= static method
    public static String getLogStage() {
        return ReloadManager.getReloadInstance().getStatus() != null ? ReloadManager.getReloadInstance().getStatus().getLogStage() : "";
    }

    public static void debug(String message, Object... val) {
        if (!LOGGER.isDebugEnabled()) return;
        LOGGER.debug(getLogStage() + message, val);
    }

    public static void briefInfo(String message) {
        LOGGER.info(getLogStage() + message);
    }

    public static void graceInfo(String message) {
        String newMessage = message.replace("dble", "OBsharding-D");
        LOGGER.info("[RL][NONE] " + newMessage);
    }

    public static void infoList2(String message, Set<String> keySet) {
        String sb = keySet == null ? "" : StringUtil.join(keySet, ",");
        LOGGER.info(getLogStage() + message + sb);
    }

    public static void warn2(String message) {
        LOGGER.warn(getLogStage() + message);
    }

    public static void warn2(String message, Object vals) {
        LOGGER.warn(getLogStage() + message, vals);
    }

    public static void warn2(String message, Exception ex) {
        LOGGER.warn(getLogStage() + message, ex);
    }
}
