/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.util;

import com.oceanbase.obsharding_d.config.model.SystemConfig;
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

    public static void printLocation(Object context) {
        if (LOGGER.isDebugEnabled()) {
            try {
                throw new DebugPrinter();
            } catch (DebugPrinter e) {
                LOGGER.debug("location:, context:{}", context, e);
            }

        }
    }


    @SuppressWarnings("AlibabaExceptionClassShouldEndWithException")
    public static class DebugPrinter extends Exception {
        public DebugPrinter(String message) {
            super(message);
        }

        public DebugPrinter() {
            super("used for debugger");
        }
    }

}
