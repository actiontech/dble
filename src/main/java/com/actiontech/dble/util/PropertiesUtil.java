/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

public final class PropertiesUtil {
    public static final Logger LOGGER = LoggerFactory.getLogger(PropertiesUtil.class);
    private PropertiesUtil() {

    }

    public static void storeProperties(Properties properties, String configFile) throws IOException {
        FileOutputStream out = null;
        try {
            File file = new File(PropertiesUtil.class.getResource(configFile).getFile());
            out = new FileOutputStream(file);
            properties.store(out, "");
            LOGGER.info("set to file success:" + configFile);
        } catch (Exception e) {
            LOGGER.warn("set to file failure", e);
            throw e;
        } finally {
            try {
                if (out != null) {
                    out.close();
                }
            } catch (Exception e) {
                LOGGER.warn("close file error");
            }
        }
    }
}
