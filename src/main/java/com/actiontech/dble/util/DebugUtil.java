/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.util;

import com.actiontech.dble.config.model.SystemConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public final class DebugUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(DebugUtil.class);

    private DebugUtil() {
    }

    public static String getDebugInfo(String fileName) {
        File pauseFile = new File(SystemConfig.getInstance().getHomePath() + File.separator + "conf", fileName);
        if (!pauseFile.exists()) {
            LOGGER.debug(pauseFile.getAbsolutePath() + " is not exists");
            return null;
        }
        String line = null;
        FileInputStream fis = null;
        InputStreamReader reader = null;
        BufferedReader in = null;
        try {
            fis = new FileInputStream(pauseFile);
            reader = new InputStreamReader(fis, "UTF-8");
            in = new BufferedReader(reader);
            line = in.readLine();
        } catch (IOException e) {
            LOGGER.warn("getDebugInfo error", e);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    //ignore error
                }
            }
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    //ignore error
                }
            }
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException e) {
                    //ignore error
                }
            }
        }

        return line;
    }
}
