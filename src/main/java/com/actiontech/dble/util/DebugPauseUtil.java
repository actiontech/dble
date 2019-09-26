/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.util;

import com.actiontech.dble.config.model.SystemConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;

public final class DebugPauseUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(DebugPauseUtil.class);

    private DebugPauseUtil() {
    }

    public static String getPauseInfo(String fileName) {
        File pauseFile = new File(SystemConfig.getHomePath() + File.separator + "conf", fileName);
        if (!pauseFile.exists()) {
            LOGGER.debug(pauseFile.getAbsolutePath() + " is not exists");
            return null;
        }
        String line = null;
        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(pauseFile), "UTF-8"));

            line = in.readLine();
            in.close();
        } catch (Exception e) {
            LOGGER.warn("getPauseInfo error", e);
        }

        return line;
    }
}
